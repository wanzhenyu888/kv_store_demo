package compact

import (
	"bytes"
	"container/heap"
	"os"

	"kv_store_demo/internal/record"
	"kv_store_demo/internal/sstable"
)

type heapItem struct {
	rec     record.Record
	tableID int
	iter    *sstable.Iterator
}

type recordHeap []heapItem

func (h recordHeap) Len() int {
	return len(h)
}

func (h recordHeap) Less(i, j int) bool {
	cmp := bytes.Compare(h[i].rec.Key, h[j].rec.Key)
	if cmp != 0 {
		return cmp < 0
	}
	return h[i].tableID > h[j].tableID
}

func (h recordHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *recordHeap) Push(x any) {
	*h = append(*h, x.(heapItem))
}

func (h *recordHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}

func advance(h *recordHeap, item heapItem) error {
	item.iter.Next()
	if err := item.iter.Err(); err != nil {
		return err
	}
	if item.iter.Valid() {
		heap.Push(h, heapItem{
			rec:     item.iter.Record(),
			tableID: item.tableID,
			iter:    item.iter,
		})
	}
	return nil
}

func Run(sstables []*sstable.SSTable, outputPath string) (*sstable.SSTable, error) {
	var iters []*sstable.Iterator
	closeIters := func() {
		for _, it := range iters {
			_ = it.Close()
		}
	}

	h := &recordHeap{}
	heap.Init(h)

	for i, sst := range sstables {
		it, err := sst.NewIterator()
		if err != nil {
			closeIters()
			return nil, err
		}
		iters = append(iters, it)
		if err := it.Err(); err != nil {
			closeIters()
			return nil, err
		}
		if it.Valid() {
			heap.Push(h, heapItem{
				rec:     it.Record(),
				tableID: i,
				iter:    it,
			})
		}
	}

	writer, err := sstable.CreateSSTableWriter(outputPath)
	if err != nil {
		closeIters()
		return nil, err
	}

	cleanup := func() {
		_ = writer.Close()
		closeIters()
		_ = os.Remove(outputPath)
	}

	wrote := false
	for h.Len() > 0 {
		first := heap.Pop(h).(heapItem)
		key := append([]byte(nil), first.rec.Key...)
		latest := first.rec

		if err := advance(h, first); err != nil {
			cleanup()
			return nil, err
		}

		for h.Len() > 0 && bytes.Equal((*h)[0].rec.Key, key) {
			item := heap.Pop(h).(heapItem)
			if err := advance(h, item); err != nil {
				cleanup()
				return nil, err
			}
		}

		if latest.Type == record.TypePut {
			if err := writer.Append(latest); err != nil {
				cleanup()
				return nil, err
			}
			wrote = true
		}
	}

	if err := writer.Close(); err != nil {
		closeIters()
		_ = os.Remove(outputPath)
		return nil, err
	}
	closeIters()

	if !wrote {
		_ = os.Remove(outputPath)
		return nil, nil
	}

	sst, err := sstable.Open(0, outputPath)
	if err != nil {
		_ = os.Remove(outputPath)
		return nil, err
	}
	return sst, nil
}
