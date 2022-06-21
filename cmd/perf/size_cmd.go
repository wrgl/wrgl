package main

import (
	"fmt"
	"io"
	"math"
	"path/filepath"
	"sort"

	"github.com/dgraph-io/badger/v3"
	"github.com/spf13/cobra"
	"github.com/wrgl/wrgl/pkg/objects"
)

func sizeCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "size WRGL_DIRECTORY",
		Short: "Measure size and statistics of different types of object in WRGL_DIRECTORY.",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			db, err := badger.Open(
				badger.DefaultOptions(filepath.Join(args[0], "kv")).
					WithLoggingLevel(badger.ERROR),
			)
			if err != nil {
				return err
			}
			defer db.Close()

			prefixes := objects.Prefixes()

			keySizes := []int64{}
			valSizes := []int64{}
			cumKeySizes := []int64{}
			cumValSizes := []int64{}
			statMap := map[string]*sizeStat{}
			for _, prefix := range prefixes {
				keySizes = keySizes[:0]
				valSizes = valSizes[:0]

				if err = db.View(func(txn *badger.Txn) error {
					opt := badger.DefaultIteratorOptions
					opt.Prefix = []byte(prefix)
					it := txn.NewIterator(opt)
					defer it.Close()
					for it.Rewind(); it.Valid(); it.Next() {
						item := it.Item()
						keySizes = append(keySizes, item.KeySize())
						valSizes = append(valSizes, item.ValueSize())
					}
					return nil
				}); err != nil {
					return err
				}

				statMap[prefix] = calculateSizeStat(keySizes, valSizes)

				cumKeySizes = append(cumKeySizes, keySizes...)
				cumValSizes = append(cumValSizes, valSizes...)
			}

			total := len(cumKeySizes)
			for prefix, stat := range statMap {
				cmd.Printf("Prefix %q: %d (%s) objects\n", prefix, stat.Count, percentage(uint64(stat.Count), uint64(total)))
				stat.Print(cmd.OutOrStdout())
				cmd.Println()
			}
			cummulativeStat := calculateSizeStat(cumKeySizes, cumValSizes)
			cmd.Printf("Cummulative: %d objects\n", total)
			cummulativeStat.Print(cmd.OutOrStdout())
			return nil
		},
	}
	return cmd
}

func percentage(count, total uint64) string {
	return fmt.Sprintf("%d%%", uint64(math.Round(float64(count)/float64(total)*100)))
}

type sizeStat struct {
	Count     int
	KeyMedian uint64
	KeyMean   uint64
	KeyStdDev uint64
	ValMedian uint64
	ValMean   uint64
	ValStdDev uint64
}

func calculateSizeStat(keySizes, valSizes []int64) *sizeStat {
	s := &sizeStat{}
	var totalKeySize uint64
	var totalValSize uint64
	for i, v := range keySizes {
		totalKeySize += uint64(v)
		totalValSize += uint64(valSizes[i])
	}
	s.Count = len(keySizes)
	sort.Slice(keySizes, func(i, j int) bool { return keySizes[i] < keySizes[j] })
	sort.Slice(valSizes, func(i, j int) bool { return valSizes[i] < valSizes[j] })
	s.KeyMean = uint64(math.Round(float64(totalKeySize) / float64(s.Count)))
	s.ValMean = uint64(math.Round(float64(totalValSize) / float64(s.Count)))
	s.KeyStdDev = standardDeviation(keySizes, int64(s.KeyMean))
	s.ValStdDev = standardDeviation(valSizes, int64(s.ValMean))
	s.KeyMedian = uint64(keySizes[s.Count/2])
	s.ValMedian = uint64(valSizes[s.Count/2])
	return s
}

func (s *sizeStat) Print(out io.Writer) {
	fmt.Fprintf(out, "Key mean: \t%s\t\tVal mean: \t%s\n", humanSize(s.KeyMean), humanSize(s.ValMean))
	fmt.Fprintf(out, "Key median: \t%s\t\tVal median: \t%s\n", humanSize(s.KeyMedian), humanSize(s.ValMedian))
	fmt.Fprintf(out, "Key stddev: \t%s (%s)\tVal stddev: \t%s (%s)\n",
		humanSize(s.KeyStdDev),
		percentage(s.KeyStdDev, s.KeyMean),
		humanSize(s.ValStdDev),
		percentage(s.ValStdDev, s.ValMean),
	)
}

func humanSize(v uint64) string {
	if v >= 1<<30 {
		return fmt.Sprintf("%.2f Gb", float64(v)/float64(1<<30))
	}
	if v >= 1<<20 {
		return fmt.Sprintf("%.2f Mb", float64(v)/float64(1<<20))
	}
	if v >= 1<<10 {
		return fmt.Sprintf("%.2f Kb", float64(v)/float64(1<<10))
	}
	return fmt.Sprintf("%d B", v)
}

func standardDeviation(values []int64, mean int64) uint64 {
	var sum uint64
	for _, v := range values {
		sum += uint64(math.Abs(float64((v - mean)))) ^ 2
	}
	return uint64(math.Round(math.Sqrt(float64(sum) / float64(len(values)))))
}
