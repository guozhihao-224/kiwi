/*
 * Copyright (c) 2023-present, Arana/Kiwi Community.  All rights reserved.
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

package kiwi_test

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/redis/go-redis/v9"

	"github.com/OpenAtomFoundation/kiwi/tests/util"
)

var _ = Describe("List", Ordered, func() {
	var (
		ctx    = context.TODO()
		s      *util.Server
		client *redis.Client
		clientBlock *redis.Client
	)

	s2s := map[string]string{
		"key_1": "value_1",
		"key_2": "value_2",
		"key_3": "value_3",
		"key_4": "value_4",
		"key_5": "value_5",
		"key_6": "value_6",
	}

	// BeforeAll closures will run exactly once before any of the specs
	// within the Ordered container.
	BeforeAll(func() {
		config := util.GetConfPath(false, 0)
		s = util.StartServer(config, map[string]string{"port": strconv.Itoa(7777)}, true)
		Expect(s).NotTo(Equal(nil))
	})

	// AfterAll closures will run exactly once after the last spec has
	// finished running.
	AfterAll(func() {
		err := s.Close()
		if err != nil {
			log.Println("Close Server fail.", err.Error())
			return
		}
	})

	// When running each spec Ginkgo will first run the BeforeEach
	// closure and then the subject closure.Doing so ensures that
	// each spec has a pristine, correctly initialized, copy of the
	// shared variable.
	BeforeEach(func() {
		client = s.NewClient()
		clientBlock = s.NewClient()
		// TODO don't assert FlushDB's result, bug will fixed by issue #401
		//Expect(client.FlushDB(ctx).Err()).NotTo(HaveOccurred())
		if res := client.FlushDB(ctx); res.Err() != nil {
			fmt.Println("[List]FlushDB error: ", res.Err())
		}
		if res := clientBlock.FlushDB(ctx); res.Err() != nil {
			fmt.Println("[List]FlushDB error: ", res.Err())
		}
		time.Sleep(1 * time.Second)
	})

	// nodes that run after the spec's subject(It).
	AfterEach(func() {
		err := client.Close()
		if err != nil {
			log.Println("Close client conn fail.", err.Error())
			return
		}
		err = clientBlock.Close()
		if err != nil {
			log.Println("Close client conn fail.", err.Error())
			return
		}
	})
	//TODO(dingxiaoshuai) Add more test cases.
	It("Cmd LPUSH", func() {
		log.Println("Cmd LPUSH Begin")

		Expect(client.LPush(ctx, DefaultKey, s2s["key_2"]).Val()).To(Equal(int64(1)))
		Expect(client.LPush(ctx, DefaultKey, s2s["key_1"]).Val()).To(Equal(int64(2)))

		Expect(client.LRange(ctx, DefaultKey, 0, -1).Val()).To(Equal([]string{s2s["key_1"], s2s["key_2"]}))

		//del
		del := client.Del(ctx, DefaultKey)
		Expect(del.Err()).NotTo(HaveOccurred())
	})

	It("Cmd LPUSHX", func() {
		Expect(client.LPushX(ctx, DefaultKey, s2s["key_1"]).Val()).To(Equal(int64(0)))
		Expect(client.LRange(ctx, DefaultKey, 0, -1).Val()).To(Equal([]string{}))

		Expect(client.LPush(ctx, DefaultKey, s2s["key_2"]).Val()).To(Equal(int64(1)))
		Expect(client.LPushX(ctx, DefaultKey, s2s["key_3"]).Val()).To(Equal(int64(2)))
		Expect(client.LRange(ctx, DefaultKey, 0, -1).Val()).To(Equal([]string{s2s["key_3"], s2s["key_2"]}))

		Expect(client.LPushX(ctx, DefaultKey, s2s["key_4"], s2s["key_5"]).Val()).To(Equal(int64(4)))
		Expect(client.LRange(ctx, DefaultKey, 0, -1).Val()).To(Equal([]string{s2s["key_5"], s2s["key_4"], s2s["key_3"], s2s["key_2"]}))

		del := client.Del(ctx, DefaultKey)
		Expect(del.Err()).NotTo(HaveOccurred())
	})

	It("Cmd RPUSH", func() {
		log.Println("Cmd RPUSH Begin")
		Expect(client.RPush(ctx, DefaultKey, s2s["key_1"]).Val()).To(Equal(int64(1)))
		Expect(client.RPush(ctx, DefaultKey, s2s["key_2"]).Val()).To(Equal(int64(2)))

		Expect(client.LRange(ctx, DefaultKey, 0, -1).Val()).To(Equal([]string{s2s["key_1"], s2s["key_2"]}))
		//del
		del := client.Del(ctx, DefaultKey)
		Expect(del.Err()).NotTo(HaveOccurred())
	})

	It("Cmd RPUSHX", func() {
		Expect(client.RPushX(ctx, DefaultKey, s2s["key_1"]).Val()).To(Equal(int64(0)))
		Expect(client.LRange(ctx, DefaultKey, 0, -1).Val()).To(Equal([]string{}))

		Expect(client.RPush(ctx, DefaultKey, s2s["key_2"]).Val()).To(Equal(int64(1)))
		Expect(client.RPushX(ctx, DefaultKey, s2s["key_3"]).Val()).To(Equal(int64(2)))
		Expect(client.LRange(ctx, DefaultKey, 0, -1).Val()).To(Equal([]string{s2s["key_2"], s2s["key_3"]}))

		Expect(client.RPushX(ctx, DefaultKey, s2s["key_4"], s2s["key_5"]).Val()).To(Equal(int64(4)))
		Expect(client.LRange(ctx, DefaultKey, 0, -1).Val()).To(Equal([]string{s2s["key_2"], s2s["key_3"], s2s["key_4"], s2s["key_5"]}))

		del := client.Del(ctx, DefaultKey)
		Expect(del.Err()).NotTo(HaveOccurred())
	})

	It("Cmd LPop", func() {
		rPush := client.RPush(ctx, DefaultKey, s2s["key_1"])
		Expect(rPush.Err()).NotTo(HaveOccurred())
		rPush = client.RPush(ctx, DefaultKey, s2s["key_2"])
		Expect(rPush.Err()).NotTo(HaveOccurred())
		rPush = client.RPush(ctx, DefaultKey, s2s["key_3"])
		Expect(rPush.Err()).NotTo(HaveOccurred())

		lPop := client.LPop(ctx, DefaultKey)
		Expect(lPop.Err()).NotTo(HaveOccurred())
		Expect(lPop.Val()).To(Equal(s2s["key_1"]))

		lRange := client.LRange(ctx, DefaultKey, 0, -1)
		Expect(lRange.Err()).NotTo(HaveOccurred())
		Expect(lRange.Val()).To(Equal([]string{s2s["key_2"], s2s["key_3"]}))

		err := client.Do(ctx, "LPOP", DefaultKey, 1, 2).Err()
		Expect(err).To(MatchError(ContainSubstring("ERR wrong number of arguments for 'lpop' command")))

		del := client.Del(ctx, DefaultKey)
		Expect(del.Err()).NotTo(HaveOccurred())
	})

	It("should RPop", func() {
		rPush := client.RPush(ctx, DefaultKey, s2s["key_1"])
		Expect(rPush.Err()).NotTo(HaveOccurred())
		rPush = client.RPush(ctx, DefaultKey, s2s["key_2"])
		Expect(rPush.Err()).NotTo(HaveOccurred())
		rPush = client.RPush(ctx, DefaultKey, s2s["key_3"])
		Expect(rPush.Err()).NotTo(HaveOccurred())

		rPop := client.RPop(ctx, DefaultKey)
		Expect(rPop.Err()).NotTo(HaveOccurred())
		Expect(rPop.Val()).To(Equal(s2s["key_3"]))

		lRange := client.LRange(ctx, DefaultKey, 0, -1)
		Expect(lRange.Err()).NotTo(HaveOccurred())
		Expect(lRange.Val()).To(Equal([]string{s2s["key_1"], s2s["key_2"]}))

		err := client.Do(ctx, "RPOP", DefaultKey, 1, 2).Err()

		Expect(err).To(MatchError(ContainSubstring("ERR wrong number of arguments for 'rpop' command")))
		//del
		del := client.Del(ctx, DefaultKey)
		Expect(del.Err()).NotTo(HaveOccurred())
	})

	It("Cmd RPopLPush", func() {
		rPush := client.RPush(ctx, DefaultKey, s2s["key_1"])
		Expect(rPush.Err()).NotTo(HaveOccurred())
		rPush = client.RPush(ctx, DefaultKey, s2s["key_2"])
		Expect(rPush.Err()).NotTo(HaveOccurred())
		rPush = client.RPush(ctx, DefaultKey, s2s["key_3"])
		Expect(rPush.Err()).NotTo(HaveOccurred())

		rPopLPush := client.RPopLPush(ctx, DefaultKey, "destination_key")
		Expect(rPopLPush.Err()).NotTo(HaveOccurred())
		Expect(rPopLPush.Val()).To(Equal(s2s["key_3"]))

		lRange := client.LRange(ctx, DefaultKey, 0, -1)
		Expect(lRange.Err()).NotTo(HaveOccurred())
		Expect(lRange.Val()).To(Equal([]string{s2s["key_1"], s2s["key_2"]}))

		lRangeDest := client.LRange(ctx, "destination_key", 0, -1)
		Expect(lRangeDest.Err()).NotTo(HaveOccurred())
		Expect(lRangeDest.Val()).To(Equal([]string{s2s["key_3"]}))

		//del
		del := client.Del(ctx, DefaultKey)
		Expect(del.Err()).NotTo(HaveOccurred())
	})

	It("Cmd LRem", func() {
		rPush := client.RPush(ctx, DefaultKey, s2s["key_1"])
		Expect(rPush.Err()).NotTo(HaveOccurred())
		rPush = client.RPush(ctx, DefaultKey, s2s["key_1"])
		Expect(rPush.Err()).NotTo(HaveOccurred())
		rPush = client.RPush(ctx, DefaultKey, s2s["key_2"])
		Expect(rPush.Err()).NotTo(HaveOccurred())
		rPush = client.RPush(ctx, DefaultKey, s2s["key_1"])
		Expect(rPush.Err()).NotTo(HaveOccurred())

		lRem := client.LRem(ctx, DefaultKey, -2, s2s["key_1"])
		Expect(lRem.Err()).NotTo(HaveOccurred())
		Expect(lRem.Val()).To(Equal(int64(2)))

		lRange := client.LRange(ctx, DefaultKey, 0, -1)
		Expect(lRange.Err()).NotTo(HaveOccurred())
		Expect(lRange.Val()).To(Equal([]string{s2s["key_1"], s2s["key_2"]}))

		//del
		del := client.Del(ctx, DefaultKey)
		Expect(del.Err()).NotTo(HaveOccurred())
	})

	It("should LTrim", func() {
		rPush := client.RPush(ctx, DefaultKey, s2s["key_1"])
		Expect(rPush.Err()).NotTo(HaveOccurred())
		rPush = client.RPush(ctx, DefaultKey, s2s["key_2"])
		Expect(rPush.Err()).NotTo(HaveOccurred())
		rPush = client.RPush(ctx, DefaultKey, s2s["key_3"])
		Expect(rPush.Err()).NotTo(HaveOccurred())

		lTrim := client.LTrim(ctx, DefaultKey, 1, -1)
		Expect(lTrim.Err()).NotTo(HaveOccurred())
		Expect(lTrim.Val()).To(Equal(OK))

		lRange := client.LRange(ctx, DefaultKey, 0, -1)
		Expect(lRange.Err()).NotTo(HaveOccurred())
		Expect(lRange.Val()).To(Equal([]string{s2s["key_2"], s2s["key_3"]}))
		// del
		del := client.Del(ctx, DefaultKey)
		Expect(del.Err()).NotTo(HaveOccurred())
	})

	It("should LSet", func() {
		rPush := client.RPush(ctx, DefaultKey, s2s["key_1"])
		Expect(rPush.Err()).NotTo(HaveOccurred())
		rPush = client.RPush(ctx, DefaultKey, s2s["key_2"])
		Expect(rPush.Err()).NotTo(HaveOccurred())
		rPush = client.RPush(ctx, DefaultKey, s2s["key_3"])
		Expect(rPush.Err()).NotTo(HaveOccurred())

		lSet := client.LSet(ctx, DefaultKey, 0, s2s["key_4"])
		Expect(lSet.Err()).NotTo(HaveOccurred())
		Expect(lSet.Val()).To(Equal(OK))

		lSet = client.LSet(ctx, DefaultKey, -2, s2s["key_5"])
		Expect(lSet.Err()).NotTo(HaveOccurred())
		Expect(lSet.Val()).To(Equal(OK))

		lRange := client.LRange(ctx, DefaultKey, 0, -1)
		Expect(lRange.Err()).NotTo(HaveOccurred())
		Expect(lRange.Val()).To(Equal([]string{s2s["key_4"], s2s["key_5"], s2s["key_3"]}))

		// del
		del := client.Del(ctx, DefaultKey)
		Expect(del.Err()).NotTo(HaveOccurred())
	})

	It("should LInsert", func() {
		rPush := client.RPush(ctx, DefaultKey, s2s["key_1"])
		Expect(rPush.Err()).NotTo(HaveOccurred())
		rPush = client.RPush(ctx, DefaultKey, s2s["key_2"])
		Expect(rPush.Err()).NotTo(HaveOccurred())

		lInsert := client.LInsert(ctx, DefaultKey, "BEFORE", s2s["key_2"], s2s["key_3"])
		Expect(lInsert.Err()).NotTo(HaveOccurred())
		Expect(lInsert.Val()).To(Equal(int64(3)))

		lRange := client.LRange(ctx, DefaultKey, 0, -1)
		Expect(lRange.Err()).NotTo(HaveOccurred())
		Expect(lRange.Val()).To(Equal([]string{s2s["key_1"], s2s["key_3"], s2s["key_2"]}))

		// del
		del := client.Del(ctx, DefaultKey)
		Expect(del.Err()).NotTo(HaveOccurred())
	})

	It("SHOULD LIndex", func() {
		rPush := client.RPush(ctx, DefaultKey, s2s["key_1"])
		Expect(rPush.Err()).NotTo(HaveOccurred())
		rPush = client.RPush(ctx, DefaultKey, s2s["key_2"])
		Expect(rPush.Err()).NotTo(HaveOccurred())
		rPush = client.RPush(ctx, DefaultKey, s2s["key_3"])
		Expect(rPush.Err()).NotTo(HaveOccurred())

		lIndex := client.LIndex(ctx, DefaultKey, 0)
		Expect(lIndex.Err()).NotTo(HaveOccurred())
		Expect(lIndex.Val()).To(Equal(s2s["key_1"]))

		lIndex = client.LIndex(ctx, DefaultKey, 1)
		Expect(lIndex.Err()).NotTo(HaveOccurred())
		Expect(lIndex.Val()).To(Equal(s2s["key_2"]))

		lIndex = client.LIndex(ctx, DefaultKey, -1)
		Expect(lIndex.Err()).NotTo(HaveOccurred())
		Expect(lIndex.Val()).To(Equal(s2s["key_3"]))

		lIndex = client.LIndex(ctx, DefaultKey, 4)
		Expect(lIndex.Err()).To(SatisfyAny(
			BeNil(),
			MatchError(redis.Nil),
		))
		Expect(lIndex.Val()).To(Equal(""))

		err := client.Do(ctx, "lindex", DefaultKey, 1, 2).Err()
		Expect(err).To(MatchError(ContainSubstring("ERR wrong number of arguments for 'lindex' command")))

		del := client.Del(ctx, DefaultKey)
		Expect(del.Err()).NotTo(HaveOccurred())
	})

	It("SHOULD LLen", func() {
		lLen := client.LLen(ctx, DefaultKey)
		Expect(lLen.Err()).NotTo(HaveOccurred())
		Expect(lLen.Val()).To(Equal(int64(0)))

		rPush := client.RPush(ctx, DefaultKey, s2s["key_1"])
		Expect(rPush.Err()).NotTo(HaveOccurred())
		rPush = client.RPush(ctx, DefaultKey, s2s["key_2"])
		Expect(rPush.Err()).NotTo(HaveOccurred())

		lLen = client.LLen(ctx, DefaultKey)
		Expect(lLen.Err()).NotTo(HaveOccurred())
		Expect(lLen.Val()).To(Equal(int64(2)))

		err := client.Do(ctx, "llen", DefaultKey, 1).Err()
		Expect(err).To(MatchError(ContainSubstring("ERR wrong number of arguments for 'llen' command")))

		del := client.Del(ctx, DefaultKey)
		Expect(del.Err()).NotTo(HaveOccurred())
	})

	It("Cmd BLPOP/BRPOP single existing list", func() {
		// now blist : d, c, large, b, a
        Expect(client.LPush(ctx, "blist", "a", "b", "large", "c", "d").Err()).NotTo(HaveOccurred())
		
        brpop := client.BRPop(ctx, 0, "blist")
        Expect(brpop.Err()).NotTo(HaveOccurred())
        Expect(brpop.Val()).To(Equal([]string{"blist", "a"}))
        
        Expect(client.Del(ctx, "blist").Err()).NotTo(HaveOccurred())
    })

	It("Cmd BLPOP/BRPOP unblock by timeout/LPUSH/RPUSH/RPopLPUSH", func() {
		var wg sync.WaitGroup 
		
		// BRPOP unlock by timeout 
		wg.Add(1)
		brpop, e := clientBlock.BRPop(ctx, 1 * time.Second, "blist").Result()
		Expect(e).To(SatisfyAny(
			BeNil(),
			MatchError(redis.Nil),
		))
		Expect(brpop).To(BeEmpty())
		wg.Done()
	
		// BRPOP unlock by LPUSH
		wg.Add(1)
		go func() {
			defer GinkgoRecover()
			defer wg.Done()
			brpop := clientBlock.BRPop(ctx, 0 * time.Second, "blist-1")
			Expect(brpop.Err()).NotTo(HaveOccurred())
			Expect(brpop.Val()).To(Equal([]string{"blist-1", "foo"}))
		}()
		time.Sleep(200 * time.Millisecond)
		Expect(client.LPush(ctx, "blist-1", "foo").Val()).To(Equal(int64(1)))
		wg.Wait()

		// BRPOP unlock by RPUSH
		wg.Add(1)
		go func() {
			defer GinkgoRecover()
			defer wg.Done()
			brpop := clientBlock.BRPop(ctx, 0 * time.Second, "blist-2")
			Expect(brpop.Err()).NotTo(HaveOccurred())
			Expect(brpop.Val()).To(Equal([]string{"blist-2", "bar"}))
		}()
		time.Sleep(200 * time.Millisecond)
		Expect(client.RPush(ctx, "blist-2", "bar").Val()).To(Equal(int64(1)))
		wg.Wait()

		// BRPOP unlock by RPopLPush 
		wg.Add(1)
		go func() {
			defer GinkgoRecover()
			defer wg.Done()
			brpop := clientBlock.BRPop(ctx, 0 * time.Second, "blist-3")
			Expect(brpop.Err()).NotTo(HaveOccurred())
			Expect(brpop.Val()).To(Equal([]string{"blist-3", "car"}))
		}()
		time.Sleep(200 * time.Millisecond)
		Expect(client.LPush(ctx, "blist-1", "car").Err()).NotTo(HaveOccurred())
		Expect(client.RPopLPush(ctx, "blist-1", "blist-3").Err()).NotTo(HaveOccurred())
        wg.Wait()

        Expect(client.Del(ctx, "blist-1", "blist-2", "blist-3").Err()).NotTo(HaveOccurred())
	})

	// Select from left to right
	It("Cmd BLPOP/BRPOP multiple lists", func() {
		Expect(client.RPush(ctx, "blist-1", "a").Err()).NotTo(HaveOccurred())
        Expect(client.RPush(ctx, "blist-2", "b").Err()).NotTo(HaveOccurred())

		brpop := clientBlock.BRPop(ctx, 0 * time.Second, "blist-1", "blist-2", "blist-2", "blist-1")
		Expect(brpop.Err()).NotTo(HaveOccurred())
		Expect(brpop.Val()).To(Equal([]string{"blist-1", "a"}))

		brpop = clientBlock.BRPop(ctx, 0 * time.Second, "blist-1", "blist-2", "blist-2", "blist-1")
		Expect(brpop.Err()).NotTo(HaveOccurred())
		Expect(brpop.Val()).To(Equal([]string{"blist-2", "b"}))

		Expect(client.Del(ctx, "blist-1", "blist-2").Err()).NotTo(HaveOccurred())
	})

	// First Blocked First Served
	It("Cmd BLPOP/BRPOP serve priority", func() {
		var wg sync.WaitGroup
		results := make([][]string, 2)
		for i := range results {
			results[i] = make([]string, 2)
		}

		wg.Add(2)
		go func() {
			defer GinkgoRecover()
			defer wg.Done()
			c := s.NewClient()
			defer c.Close()

			brpop := c.BRPop(ctx, 0 * time.Second, "blist-1")
			Expect(brpop.Err()).NotTo(HaveOccurred())
			results[0] = brpop.Val()
		}()
		time.Sleep(1 * time.Second)
		go func() {
			defer GinkgoRecover()
			defer wg.Done()
			c := s.NewClient()
			defer c.Close()

			brpop := c.BRPop(ctx, 0 * time.Second, "blist-1")
			Expect(brpop.Err()).NotTo(HaveOccurred())
			results[1] = brpop.Val()
		}()
		time.Sleep(1 * time.Second)

		Expect(client.RPush(ctx, "blist-1", "v1", "v2").Err()).NotTo(HaveOccurred())
        wg.Wait()

		Expect(results[0]).To(Equal([]string{"blist-1", "v2"}))
        Expect(results[1]).To(Equal([]string{"blist-1", "v1"}))
	})
})
