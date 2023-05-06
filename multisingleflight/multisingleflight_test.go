package multisingleflight

import (
	"golang.org/x/sync/singleflight"
	"math/rand"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func Test_multisingleflight_Do(t *testing.T) {
	m := make(map[string]string)
	keys := make([]string, 0)
	batch := 50
	for i := 0; i < 100000; i++ {
		if i < 100000*0.8 {
			m[strconv.FormatInt(int64(i), 10)] = strconv.FormatInt(int64(i), 10)
		}
		keys = append(keys, strconv.FormatInt(int64(i), 10))
	}

	sharedCount := 0
	wg := sync.WaitGroup{}
	rand.Seed(time.Now().Unix())
	mu := sync.Mutex{}
	results := make([]map[string]*Result, 0)
	g := Group{}
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cloneKeys := make([]string, len(keys))
			copy(cloneKeys, keys)
			rand.Shuffle(len(cloneKeys), func(i, j int) {
				cloneKeys[i], cloneKeys[j] = cloneKeys[j], cloneKeys[i]
			})
			for j := 0; j < 100; j++ {
				start := rand.Intn(len(cloneKeys) - batch + 1)
				resultMap := g.Do(cloneKeys[start:start+batch], func(keys_ []string) map[string]*Result {
					result := make(map[string]*Result, len(keys_))
					for _, key := range keys_ {
						result[key] = &Result{Val: m[key]}
					}
					time.Sleep(time.Millisecond * time.Duration(rand.Intn(10)+5))
					return result
				})
				mu.Lock()
				results = append(results, resultMap)
				mu.Unlock()
				time.Sleep(time.Millisecond * time.Duration(rand.Intn(20)+10))
			}
		}()
	}
	wg.Wait()

	count := 0
	for _, rm := range results {
		if len(rm) != batch {
			t.Errorf("len(rm) != %v, len(rm): %v", batch, len(rm))
		}
		for key, result := range rm {
			if m[key] != result.Val {
				t.Errorf("key:%v, m[key]%v, result: %v", key, m[key], result)
			}
			if result.Shared {
				sharedCount++
			}
			count++
		}
	}

	t.Logf("sharedCount:%v, count: %v", sharedCount, count)
}

func Test_multisingleflight_DoChan(t *testing.T) {
	m := make(map[string]string)
	keys := make([]string, 0)
	batch := 50
	for i := 0; i < 100000; i++ {
		if i < 100000*0.8 {
			m[strconv.FormatInt(int64(i), 10)] = strconv.FormatInt(int64(i), 10)
		}
		keys = append(keys, strconv.FormatInt(int64(i), 10))
	}

	sharedCount := 0
	wg := sync.WaitGroup{}
	rand.Seed(time.Now().Unix())
	mu := sync.Mutex{}
	results := make([]<-chan map[string]*Result, 0)
	g := Group{}
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cloneKeys := make([]string, len(keys))
			copy(cloneKeys, keys)
			rand.Shuffle(len(cloneKeys), func(i, j int) {
				cloneKeys[i], cloneKeys[j] = cloneKeys[j], cloneKeys[i]
			})
			for j := 0; j < 100; j++ {
				start := rand.Intn(len(cloneKeys) - batch + 1)
				resultMap := g.DoChan(cloneKeys[start:start+batch], func(keys_ []string) map[string]*Result {
					result := make(map[string]*Result, len(keys_))
					for _, key := range keys_ {
						result[key] = &Result{Val: m[key]}
					}
					time.Sleep(time.Millisecond * time.Duration(rand.Intn(10)+5))
					return result
				})
				mu.Lock()
				results = append(results, resultMap)
				mu.Unlock()
				time.Sleep(time.Millisecond * time.Duration(rand.Intn(20)+10))
			}
		}()
	}
	wg.Wait()

	count := 0
	for _, rmChan := range results {
		rm := <-rmChan
		if len(rm) != batch {
			t.Errorf("len(rm) != %v, len(rm): %v", batch, len(rm))
		}
		for key, result := range rm {
			if m[key] != result.Val {
				t.Errorf("key:%v, m[key]%v, result: %v", key, m[key], result)
			}
			if result.Shared {
				sharedCount++
			}
			count++
		}
	}

	t.Logf("sharedCount:%v, count: %v", sharedCount, count)
}

func Benchmark_multisingleflight(b *testing.B) {
	rand.Seed(time.Now().Unix())
	g := Group{}
	keys := make([]string, 0)
	for i := 0; i < 1000000; i++ {
		keys = append(keys, strconv.FormatInt(int64(i), 10))
	}
	count := int64(0)
	b.SetParallelism(50)
	runtime.GOMAXPROCS(4)

	cloneKeysArray := make([][]string, 0)
	for i := 0; i < 10; i++ {
		cloneKeys := make([]string, len(keys))
		copy(cloneKeys, keys)
		rand.Shuffle(len(cloneKeys), func(i, j int) {
			cloneKeys[i], cloneKeys[j] = cloneKeys[j], cloneKeys[i]
		})
		cloneKeysArray = append(cloneKeysArray, cloneKeys)
	}

	b.RunParallel(func(pb *testing.PB) {
		atomic.AddInt64(&count, 1)
		cloneKeys := cloneKeysArray[rand.Intn(len(cloneKeysArray))]
		for pb.Next() {
			start := rand.Intn(len(cloneKeys) - 20 + 1)
			g.Do(cloneKeys[start:start+20], func(keys []string) map[string]*Result {
				result := make(map[string]*Result, len(keys))
				for _, key := range keys {
					result[key] = &Result{Val: key}
				}
				time.Sleep(time.Millisecond * 5)
				return result
			})
		}
	})
	b.Logf("count: %v", count)
}

func Benchmark_singleflight(b *testing.B) {
	g := singleflight.Group{}
	keys := make([]string, 0)
	for i := 0; i < 10000; i++ {
		keys = append(keys, strconv.FormatInt(int64(i), 10))
	}
	count := int64(0)
	b.SetParallelism(50)
	runtime.GOMAXPROCS(4)

	cloneKeysArray := make([][]string, 0)
	for i := 0; i < 10; i++ {
		cloneKeys := make([]string, len(keys))
		copy(cloneKeys, keys)
		rand.Shuffle(len(cloneKeys), func(i, j int) {
			cloneKeys[i], cloneKeys[j] = cloneKeys[j], cloneKeys[i]
		})
		cloneKeysArray = append(cloneKeysArray, cloneKeys)
	}

	b.RunParallel(func(pb *testing.PB) {
		atomic.AddInt64(&count, 1)
		cloneKeys := cloneKeysArray[rand.Intn(len(cloneKeysArray))]
		//fmt.Println(cloneKeys)
		for pb.Next() {
			index := rand.Intn(len(keys))
			g.Do(cloneKeys[index], func() (interface{}, error) {
				time.Sleep(time.Millisecond * 3)
				return "", nil
			})
		}
	})
	b.Logf("count: %v", count)
}
