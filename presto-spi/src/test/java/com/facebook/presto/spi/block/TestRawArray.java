/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.spi.block;

import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.LongArrayBlockBuilder;
//import com.facebook.presto.spi.type.BigintType;
//import com.facebook.presto.spi.type.DoubleType;
//import static org.testng.Assert.assertEquals;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import static java.lang.Double.doubleToLongBits;
import static java.lang.Double.longBitsToDouble;
import static java.lang.System.arraycopy;


import org.testng.annotations.Test;


public class TestRawArray
{

  // Evaluator class that would be generated from
  // extendedprice * (1 - discount) - quantity * supplycost
  public static class ProfitExpr {
    BlockContents extendedPrice = new BlockContents();
    BlockContents discount = new BlockContents();
    BlockContents quantity = new BlockContents();
    BlockContents supplyCost = new BlockContents();
    MapHolder mapHolder = new MapHolder();
    boolean[] nullsInReserve;
    boolean[] nullsInBatch;

    void boolArrayOr(boolean[] target, boolean[] source, int[] map, int positionCount) {
      if (map == null) {
        for (int i = 0; i < positionCount; ++i) {
          target[i] |= source[i];
        }
      } else {
        for (int i = 0; i < positionCount; ++i) {
          target[i] |= source[map[i]];
        }
      }
    }

    void addNullFlags(boolean[] nullFlags, int[] map, int positionCount)
    {
      if (nullFlags != null) {
        if (nullsInBatch == null && map == null) {
          nullsInBatch = nullFlags;
        } else {
          boolean[]  newNulls;
          if (nullsInReserve !=null && nullsInReserve.length >= positionCount) {
            newNulls = nullsInReserve;
          } else {
            newNulls = new boolean[positionCount];
            nullsInReserve = newNulls;
          }
          if (nullsInBatch != null) {
            System.arraycopy(nullsInBatch, 0, newNulls, 0, positionCount);

          }
            nullsInBatch = newNulls;
            boolArrayOr(nullsInBatch, nullFlags, map, positionCount);
            }
          }
    }

    Page Evaluate(Page page)
    {
      int positionCount = page.getPositionCount();
      extendedPrice.decodeBlock(page.getBlock(0), mapHolder);
      discount.decodeBlock(page.getBlock(1), mapHolder);
      quantity.decodeBlock(page.getBlock(2), mapHolder);
      supplyCost.decodeBlock(page.getBlock(3), mapHolder);
      long[] ep = extendedPrice.longs;
      long[] di = discount.longs;
      long[] qt = quantity.longs;
      long[] sc = supplyCost.longs;
      int[] epMap = extendedPrice.rowNumberMap;
      int[] diMap = discount.rowNumberMap;
      int[] qtMap = quantity.rowNumberMap;
      int[] scMap = supplyCost.rowNumberMap;
      long[] result = new long[positionCount];
      nullsInBatch = null;
      addNullFlags(extendedPrice.valueIsNull, extendedPrice.isIdentityMap ? null : epMap, positionCount);
      addNullFlags(discount.valueIsNull, discount.isIdentityMap ? null : diMap, positionCount);
      addNullFlags(quantity.valueIsNull, quantity.isIdentityMap ? null : qtMap, positionCount);
      addNullFlags(supplyCost.valueIsNull, supplyCost.isIdentityMap ? null : scMap, positionCount);
      boolean allIdentity = extendedPrice.isIdentityMap || discount.isIdentityMap | quantity.isIdentityMap | supplyCost.isIdentityMap;
      if (allIdentity && nullsInBatch == null) {
        for (int i = 0; i < positionCount; ++i) {
          result[i] = ep[i] * (1 - di[i]) - qt[i] * sc[i];
        }
      } else {
        for (int i = 0; i < positionCount; ++i) {
          if (nullsInBatch == null || !nullsInBatch[i]) {
            result[i] = ep[epMap[i]] * (1 - di[diMap[i]]) - qt[qtMap[i]] * sc[scMap[i]];
          }
        }
      }
      return new Page(positionCount, new LongArrayBlock(0, positionCount, (nullsInBatch != null ? nullsInBatch.clone() : null), result));
    }

    Page EvaluateRow(Page page)
    {
      Block ep = page.getBlock(0);
      Block di = page.getBlock(1);
      Block qt = page.getBlock(2);
      Block sc = page.getBlock(3);
      int positionCount = page.getPositionCount();
      LongArrayBlockBuilder res = new LongArrayBlockBuilder(null, positionCount);
      for (int i = 0; i < positionCount; ++i) {
        if (ep.isNull(i) || di.isNull(i) || qt.isNull(i) || sc.isNull(i)) {
          res.appendNull();
        } else {
          res.writeLong(
              doubleToLongBits(
                  longBitsToDouble(ep.getLong(i, 0)) * (1 - longBitsToDouble(di.getLong(i, 0))) - qt.getLong(i, 0) * longBitsToDouble(sc.getLong(i, 0))));
        }
      }
      return new Page(positionCount,  res.build());
    }
  }

  static class DataSource
  {
    Page ownedPage;

    public Page nextPage(int numberOfRows, boolean addNulls) {
      LongArrayBlockBuilder ep = new LongArrayBlockBuilder(null, numberOfRows);
      LongArrayBlockBuilder di = new LongArrayBlockBuilder(null, numberOfRows);
      LongArrayBlockBuilder qt = new LongArrayBlockBuilder(null, numberOfRows);
      LongArrayBlockBuilder sc = new LongArrayBlockBuilder(null, numberOfRows);
      for (int i = 0; i < numberOfRows; ++i) {
        if (addNulls && i % 17 == 0) {
          ep.appendNull();
        } else {
          ep.writeLong(doubleToLongBits((double)i * 10));
        }
        if (addNulls && i % 21 == 0) {
          di.appendNull();
        } else {
          di.writeLong(doubleToLongBits((double)i % 10));
        }
        if (addNulls && i % 31 == 0) {
          qt.appendNull();
        } else {
          qt.writeLong(1 + (i % 50));
        }
        if (addNulls && i % 41 == 0) {
          sc.appendNull();
        } else {
          sc.writeLong(doubleToLongBits(1 + (double)i * 9));
            }
      }
      return new Page(numberOfRows, ep.build(), di.build(), qt.build(), sc.build());
    }
  }

  @Test
  public void TestExpr()
  {
    DataSource source = new DataSource();
    ProfitExpr expr = new ProfitExpr();
    long tim = System.currentTimeMillis();
    for (int i = 0; i < 1000; ++i) {
      Page page = source.nextPage(1000, false);
      Page res = expr.Evaluate(page);
    }
    long endtim = System.currentTimeMillis();
    System.out.println("Time1: " + (endtim - tim));
  }

  @Test
    public void testSlice()
    {
      Slice slice = Slices.allocateDirect(10000);
        long sum = 0;
        for (int i = 0; i < 2000000; ++i) {
            sum += testMem(slice);
        }
        System.out.println("Sum " + sum);
    }

    static long testMem(Slice slice)
    {
        slice.setLong(10, 11);
        slice.setLong(18, 11);
        return slice.getLong(10) + slice.getLong(18);
    }

    static void main(String[] args)
    {
        Slice slice = Slices.allocateDirect(10000);
        long sum = 0;
        for (int i = 0; i < 20000; ++i) {
            sum += testMem(slice);
        }
        System.out.println("Sum " + sum);
    }
}
