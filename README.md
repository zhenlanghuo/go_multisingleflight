# go_multisingleflight

在官方 [singleflight](https://github.com/golang/sync/tree/master/singleflight) 源码基础上进行改造适用于批量操作的multisingleflight

使用范例
``` Go
g := Group{}
keys := make([]string, 0)
resultMap := g.Do(keys, func(keys_ []string) map[string]*Result {
    result := make(map[string]*Result, len(keys_))
    // 批量查询操作
    time.Sleep(time.Millisecond * time.Duration(rand.Intn(10)+5))
    return result
})
``` 
