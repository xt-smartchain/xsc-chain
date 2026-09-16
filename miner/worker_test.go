// Copyright 2018 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package miner

import (
	"errors"
	"math/big"
	"math/rand"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/accounts"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/consensus"
	"github.com/ethereum/go-ethereum/consensus/clique"
	"github.com/ethereum/go-ethereum/consensus/ethash"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/event"
	"github.com/ethereum/go-ethereum/params"
)

const (
	// testCode is the testing contract binary code which will initialises some
	// variables in constructor
	testCode = "0x60806040527fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff0060005534801561003457600080fd5b5060fc806100436000396000f3fe6080604052348015600f57600080fd5b506004361060325760003560e01c80630c4dae8814603757806398a213cf146053575b600080fd5b603d607e565b6040518082815260200191505060405180910390f35b607c60048036036020811015606757600080fd5b81019080803590602001909291905050506084565b005b60005481565b806000819055507fe9e44f9f7da8c559de847a3232b57364adc0354f15a2cd8dc636d54396f9587a6000546040518082815260200191505060405180910390a15056fea265627a7a723058208ae31d9424f2d0bc2a3da1a5dd659db2d71ec322a17db8f87e19e209e3a1ff4a64736f6c634300050a0032"

	// testGas is the gas required for contract deployment.
	testGas = 144109
)

var (
	// Test chain configurations
	testTxPoolConfig  core.TxPoolConfig
	ethashChainConfig *params.ChainConfig
	cliqueChainConfig *params.ChainConfig

	// Test accounts
	testBankKey, _  = crypto.GenerateKey()
	testBankAddress = crypto.PubkeyToAddress(testBankKey.PublicKey)
	testBankFunds   = big.NewInt(1000000000000000000)

	testUserKey, _  = crypto.GenerateKey()
	testUserAddress = crypto.PubkeyToAddress(testUserKey.PublicKey)

	// Test transactions
	pendingTxs []*types.Transaction
	newTxs     []*types.Transaction

	testConfig = &Config{
		Recommit: time.Second,
		GasFloor: params.GenesisGasLimit,
		GasCeil:  params.GenesisGasLimit,
	}
)

func init() {
	testTxPoolConfig = core.DefaultTxPoolConfig
	testTxPoolConfig.Journal = ""
	ethashChainConfig = params.TestChainConfig
	cliqueChainConfig = params.TestChainConfig
	cliqueChainConfig.Clique = &params.CliqueConfig{
		Period: 10,
		Epoch:  30000,
	}

	signer := types.LatestSigner(params.TestChainConfig)
	tx1 := types.MustSignNewTx(testBankKey, signer, &types.AccessListTx{
		ChainID: params.TestChainConfig.ChainID,
		Nonce:   0,
		To:      &testUserAddress,
		Value:   big.NewInt(1000),
		Gas:     params.TxGas,
	})
	pendingTxs = append(pendingTxs, tx1)

	tx2 := types.MustSignNewTx(testBankKey, signer, &types.LegacyTx{
		Nonce: 1,
		To:    &testUserAddress,
		Value: big.NewInt(1000),
		Gas:   params.TxGas,
	})
	newTxs = append(newTxs, tx2)

	rand.Seed(time.Now().UnixNano())
}

// testWorkerBackend implements worker.Backend interfaces and wraps all information needed during the testing.
type testWorkerBackend struct {
	db         ethdb.Database
	txPool     *core.TxPool
	chain      *core.BlockChain
	testTxFeed event.Feed
	genesis    *core.Genesis
	uncleBlock *types.Block
}

func newTestWorkerBackend(t *testing.T, chainConfig *params.ChainConfig, engine consensus.Engine, db ethdb.Database, n int) *testWorkerBackend {
	var gspec = core.Genesis{
		Config: chainConfig,
		Alloc:  core.GenesisAlloc{testBankAddress: {Balance: testBankFunds}},
	}

	switch e := engine.(type) {
	case *clique.Clique:
		gspec.ExtraData = make([]byte, 32+common.AddressLength+crypto.SignatureLength)
		copy(gspec.ExtraData[32:32+common.AddressLength], testBankAddress.Bytes())
		e.Authorize(testBankAddress, func(account accounts.Account, s string, data []byte) ([]byte, error) {
			return crypto.Sign(crypto.Keccak256(data), testBankKey)
		})
	case *ethash.Ethash:
	case *mockPoSAEngine:
		// Wraps ethash; no engine-specific genesis setup required.
	default:
		t.Fatalf("unexpected consensus engine type: %T", engine)
	}
	genesis := gspec.MustCommit(db)

	chain, _ := core.NewBlockChain(db, &core.CacheConfig{TrieDirtyDisabled: true}, gspec.Config, engine, vm.Config{}, nil, nil)
	txpool := core.NewTxPool(testTxPoolConfig, chainConfig, chain)

	// Generate a small n-block chain and an uncle block for it
	if n > 0 {
		blocks, _ := core.GenerateChain(chainConfig, genesis, engine, db, n, func(i int, gen *core.BlockGen) {
			gen.SetCoinbase(testBankAddress)
		})
		if _, err := chain.InsertChain(blocks); err != nil {
			t.Fatalf("failed to insert origin chain: %v", err)
		}
	}
	parent := genesis
	if n > 0 {
		parent = chain.GetBlockByHash(chain.CurrentBlock().ParentHash())
	}
	blocks, _ := core.GenerateChain(chainConfig, parent, engine, db, 1, func(i int, gen *core.BlockGen) {
		gen.SetCoinbase(testUserAddress)
	})

	return &testWorkerBackend{
		db:         db,
		chain:      chain,
		txPool:     txpool,
		genesis:    &gspec,
		uncleBlock: blocks[0],
	}
}

func (b *testWorkerBackend) BlockChain() *core.BlockChain { return b.chain }
func (b *testWorkerBackend) TxPool() *core.TxPool         { return b.txPool }

func (b *testWorkerBackend) newRandomUncle() *types.Block {
	var parent *types.Block
	cur := b.chain.CurrentBlock()
	if cur.NumberU64() == 0 {
		parent = b.chain.Genesis()
	} else {
		parent = b.chain.GetBlockByHash(b.chain.CurrentBlock().ParentHash())
	}
	blocks, _ := core.GenerateChain(b.chain.Config(), parent, b.chain.Engine(), b.db, 1, func(i int, gen *core.BlockGen) {
		var addr = make([]byte, common.AddressLength)
		rand.Read(addr)
		gen.SetCoinbase(common.BytesToAddress(addr))
	})
	return blocks[0]
}

func (b *testWorkerBackend) newRandomTx(creation bool) *types.Transaction {
	var tx *types.Transaction
	if creation {
		tx, _ = types.SignTx(types.NewContractCreation(b.txPool.Nonce(testBankAddress), big.NewInt(0), testGas, nil, common.FromHex(testCode)), types.HomesteadSigner{}, testBankKey)
	} else {
		tx, _ = types.SignTx(types.NewTransaction(b.txPool.Nonce(testBankAddress), testUserAddress, big.NewInt(1000), params.TxGas, nil, nil), types.HomesteadSigner{}, testBankKey)
	}
	return tx
}

func newTestWorker(t *testing.T, chainConfig *params.ChainConfig, engine consensus.Engine, db ethdb.Database, blocks int) (*worker, *testWorkerBackend) {
	backend := newTestWorkerBackend(t, chainConfig, engine, db, blocks)
	backend.txPool.AddLocals(pendingTxs)
	w := newWorker(testConfig, chainConfig, engine, backend, new(event.TypeMux), nil, false)
	w.setEtherbase(testBankAddress)
	return w, backend
}

// mockPoSAEngine wraps ethash with the consensus.PoSA interface so that the
// miner takes the consensus-level transaction validation path. ValidateTx
// denies the configured addresses, or returns infraErr to simulate the
// blacklist being unreadable (e.g. the system contract call failing).
type mockPoSAEngine struct {
	*ethash.Ethash

	denied   map[common.Address]bool
	infraErr error
	calls    int
}

func (m *mockPoSAEngine) PreHandle(chain consensus.ChainHeaderReader, header *types.Header, statedb *state.StateDB) error {
	return nil
}

func (m *mockPoSAEngine) CanCreate(statedb consensus.StateReader, addr common.Address, height *big.Int) bool {
	return true
}

func (m *mockPoSAEngine) ValidateTx(tx *types.Transaction, header *types.Header, parentState *state.StateDB) error {
	m.calls++
	if m.infraErr != nil {
		return m.infraErr
	}
	from, err := types.Sender(types.LatestSigner(ethashChainConfig), tx)
	if err != nil {
		return err
	}
	if m.denied[from] {
		return consensus.ErrAddressDenied
	}
	if to := tx.To(); to != nil && m.denied[*to] {
		return consensus.ErrAddressDenied
	}
	return nil
}

func (m *mockPoSAEngine) IsSystemTransaction(tx *types.Transaction, header *types.Header) (bool, error) {
	return false, nil
}

func (m *mockPoSAEngine) IsSystemContract(to *common.Address) bool { return false }

func (m *mockPoSAEngine) EnoughDistance(chain consensus.ChainReader, header *types.Header) bool {
	return true
}

func (m *mockPoSAEngine) IsLocalBlock(header *types.Header) bool { return false }

// commitWithPoSA assembles a block with the given engine over the transactions
// produced by build, and reports which of them made it into the block.
func commitWithPoSA(t *testing.T, engine *mockPoSAEngine, build func(signer types.Signer) types.Transactions) []*types.Transaction {
	t.Helper()

	// Deliberately not newTestWorker: that seeds the pool with pendingTxs, and the
	// resulting NewTxsEvent lets the worker's own mainLoop call commitTransactions
	// as soon as w.current is set, racing this goroutine over the same environment.
	backend := newTestWorkerBackend(t, ethashChainConfig, engine, rawdb.NewMemoryDatabase(), 0)
	w := newWorker(testConfig, ethashChainConfig, engine, backend, new(event.TypeMux), nil, false)
	w.setEtherbase(testBankAddress)
	defer w.close()

	parent := w.chain.CurrentBlock()
	num := parent.Number()
	header := &types.Header{
		ParentHash: parent.Hash(),
		Number:     num.Add(num, common.Big1),
		GasLimit:   core.CalcGasLimit(parent, w.config.GasFloor, w.config.GasCeil),
		Time:       parent.Time() + 1,
	}
	if err := w.engine.Prepare(w.chain, header); err != nil {
		t.Fatalf("failed to prepare header: %v", err)
	}
	if err := w.makeCurrent(parent, header); err != nil {
		t.Fatalf("failed to create mining context: %v", err)
	}
	defer w.current.state.StopPrefetcher()

	byAccount := make(map[common.Address]types.Transactions)
	for _, tx := range build(w.current.signer) {
		from, err := types.Sender(w.current.signer, tx)
		if err != nil {
			t.Fatalf("failed to recover sender: %v", err)
		}
		byAccount[from] = append(byAccount[from], tx)
	}
	w.commitTransactions(types.NewTransactionsByPriceAndNonce(w.current.signer, byAccount), testBankAddress, nil)

	return w.current.txs
}

// TestCommitTransactionsRespectsPoSAValidation checks that the miner consults
// the consensus engine before packing a transaction. Without it the miner can
// seal a block containing a blacklisted transaction, which every other node
// rejects wholesale in StateProcessor.Process, costing the validator the slot.
func TestCommitTransactionsRespectsPoSAValidation(t *testing.T) {
	denied := common.HexToAddress("0x000000000000000000000000000000000000dead")

	build := func(signer types.Signer) types.Transactions {
		return types.Transactions{
			types.MustSignNewTx(testBankKey, signer, &types.LegacyTx{
				Nonce: 0, To: &testUserAddress, Value: big.NewInt(1000),
				Gas: params.TxGas, GasPrice: big.NewInt(1),
			}),
			types.MustSignNewTx(testBankKey, signer, &types.LegacyTx{
				Nonce: 1, To: &denied, Value: big.NewInt(1000),
				Gas: params.TxGas, GasPrice: big.NewInt(1),
			}),
		}
	}

	// Control case. Without it the assertions below could pass simply because
	// nothing ever gets committed, making them vacuous.
	t.Run("empty blacklist commits everything", func(t *testing.T) {
		engine := &mockPoSAEngine{Ethash: ethash.NewFaker()}
		committed := commitWithPoSA(t, engine, build)
		if len(committed) != 2 {
			t.Fatalf("expected 2 committed transactions, got %d", len(committed))
		}
		if engine.calls == 0 {
			t.Fatal("ValidateTx was never called: the miner is not consulting consensus validation")
		}
	})

	t.Run("denied recipient is skipped", func(t *testing.T) {
		engine := &mockPoSAEngine{
			Ethash: ethash.NewFaker(),
			denied: map[common.Address]bool{denied: true},
		}
		committed := commitWithPoSA(t, engine, build)
		for _, tx := range committed {
			if to := tx.To(); to != nil && *to == denied {
				t.Fatalf("committed a transaction to blacklisted address %x", denied)
			}
		}
		if len(committed) != 1 {
			t.Fatalf("expected only the allowed transaction to be committed, got %d", len(committed))
		}
	})

	// An unreadable blacklist means "state unknown", not "this transaction is
	// denied". Treating it per-transaction would re-run the uncached system
	// contract call for every sender, every sealing round.
	//
	// Two distinct senders are required to observe that: TransactionsByPriceAndNonce
	// holds one heap entry per account, so with a single sender one Pop() empties
	// the heap and looks exactly like stopping altogether.
	t.Run("unreadable blacklist fails fast", func(t *testing.T) {
		buildTwoSenders := func(signer types.Signer) types.Transactions {
			return types.Transactions{
				types.MustSignNewTx(testBankKey, signer, &types.LegacyTx{
					Nonce: 0, To: &testUserAddress, Value: big.NewInt(1000),
					Gas: params.TxGas, GasPrice: big.NewInt(2),
				}),
				types.MustSignNewTx(testUserKey, signer, &types.LegacyTx{
					Nonce: 0, To: &testBankAddress, Value: big.NewInt(1000),
					Gas: params.TxGas, GasPrice: big.NewInt(1),
				}),
			}
		}

		engine := &mockPoSAEngine{
			Ethash:   ethash.NewFaker(),
			infraErr: errors.New("system contract unavailable"),
		}
		committed := commitWithPoSA(t, engine, buildTwoSenders)
		if len(committed) != 0 {
			t.Fatalf("expected no committed transactions, got %d", len(committed))
		}
		// Dropping only the offending account would go on to validate the second
		// sender, re-running the uncached system contract call.
		if engine.calls != 1 {
			t.Fatalf("expected ValidateTx to be called once (fail fast), got %d", engine.calls)
		}
	})
}

func TestGenerateBlockAndImportEthash(t *testing.T) {
	testGenerateBlockAndImport(t, false)
}

func TestGenerateBlockAndImportClique(t *testing.T) {
	testGenerateBlockAndImport(t, true)
}

func testGenerateBlockAndImport(t *testing.T, isClique bool) {
	var (
		engine      consensus.Engine
		chainConfig *params.ChainConfig
		db          = rawdb.NewMemoryDatabase()
	)
	if isClique {
		chainConfig = params.AllCliqueProtocolChanges
		chainConfig.Clique = &params.CliqueConfig{Period: 1, Epoch: 30000}
		engine = clique.New(chainConfig.Clique, db)
	} else {
		chainConfig = params.AllEthashProtocolChanges
		engine = ethash.NewFaker()
	}

	w, b := newTestWorker(t, chainConfig, engine, db, 0)
	defer w.close()

	// This test chain imports the mined blocks.
	db2 := rawdb.NewMemoryDatabase()
	b.genesis.MustCommit(db2)
	chain, _ := core.NewBlockChain(db2, nil, b.chain.Config(), engine, vm.Config{}, nil, nil)
	defer chain.Stop()

	// Ignore empty commit here for less noise.
	w.skipSealHook = func(task *task) bool {
		return len(task.receipts) == 0
	}

	// Wait for mined blocks.
	sub := w.mux.Subscribe(core.NewMinedBlockEvent{})
	defer sub.Unsubscribe()

	// Start mining!
	w.start()

	for i := 0; i < 5; i++ {
		b.txPool.AddLocal(b.newRandomTx(true))
		b.txPool.AddLocal(b.newRandomTx(false))
		w.postSideBlock(core.ChainSideEvent{Block: b.newRandomUncle()})
		w.postSideBlock(core.ChainSideEvent{Block: b.newRandomUncle()})

		select {
		case ev := <-sub.Chan():
			block := ev.Data.(core.NewMinedBlockEvent).Block
			if _, err := chain.InsertChain([]*types.Block{block}); err != nil {
				t.Fatalf("failed to insert new mined block %d: %v", block.NumberU64(), err)
			}
		case <-time.After(3 * time.Second): // Worker needs 1s to include new changes.
			t.Fatalf("timeout")
		}
	}
}

func TestAdjustIntervalEthash(t *testing.T) {
	testAdjustInterval(t, ethashChainConfig, ethash.NewFaker())
}

func TestAdjustIntervalClique(t *testing.T) {
	testAdjustInterval(t, cliqueChainConfig, clique.New(cliqueChainConfig.Clique, rawdb.NewMemoryDatabase()))
}

func testAdjustInterval(t *testing.T, chainConfig *params.ChainConfig, engine consensus.Engine) {
	defer engine.Close()

	w, _ := newTestWorker(t, chainConfig, engine, rawdb.NewMemoryDatabase(), 0)
	defer w.close()

	w.skipSealHook = func(task *task) bool {
		return true
	}
	w.fullTaskHook = func() {
		time.Sleep(100 * time.Millisecond)
	}
	var (
		progress = make(chan struct{}, 10)
		result   = make([]float64, 0, 10)
		index    = 0
		start    uint32
	)
	w.resubmitHook = func(minInterval time.Duration, recommitInterval time.Duration) {
		// Short circuit if interval checking hasn't started.
		if atomic.LoadUint32(&start) == 0 {
			return
		}
		var wantMinInterval, wantRecommitInterval time.Duration

		switch index {
		case 0:
			wantMinInterval, wantRecommitInterval = 3*time.Second, 3*time.Second
		case 1:
			origin := float64(3 * time.Second.Nanoseconds())
			estimate := origin*(1-intervalAdjustRatio) + intervalAdjustRatio*(origin/0.8+intervalAdjustBias)
			wantMinInterval, wantRecommitInterval = 3*time.Second, time.Duration(estimate)*time.Nanosecond
		case 2:
			estimate := result[index-1]
			min := float64(3 * time.Second.Nanoseconds())
			estimate = estimate*(1-intervalAdjustRatio) + intervalAdjustRatio*(min-intervalAdjustBias)
			wantMinInterval, wantRecommitInterval = 3*time.Second, time.Duration(estimate)*time.Nanosecond
		case 3:
			wantMinInterval, wantRecommitInterval = time.Second, time.Second
		}

		// Check interval
		if minInterval != wantMinInterval {
			t.Errorf("resubmit min interval mismatch: have %v, want %v ", minInterval, wantMinInterval)
		}
		if recommitInterval != wantRecommitInterval {
			t.Errorf("resubmit interval mismatch: have %v, want %v", recommitInterval, wantRecommitInterval)
		}
		result = append(result, float64(recommitInterval.Nanoseconds()))
		index += 1
		progress <- struct{}{}
	}
	w.start()

	time.Sleep(time.Second) // Ensure two tasks have been summitted due to start opt
	atomic.StoreUint32(&start, 1)

	w.setRecommitInterval(3 * time.Second)
	select {
	case <-progress:
	case <-time.NewTimer(time.Second).C:
		t.Error("interval reset timeout")
	}

	w.resubmitAdjustCh <- &intervalAdjust{inc: true, ratio: 0.8}
	select {
	case <-progress:
	case <-time.NewTimer(time.Second).C:
		t.Error("interval reset timeout")
	}

	w.resubmitAdjustCh <- &intervalAdjust{inc: false}
	select {
	case <-progress:
	case <-time.NewTimer(time.Second).C:
		t.Error("interval reset timeout")
	}

	w.setRecommitInterval(500 * time.Millisecond)
	select {
	case <-progress:
	case <-time.NewTimer(time.Second).C:
		t.Error("interval reset timeout")
	}
}
