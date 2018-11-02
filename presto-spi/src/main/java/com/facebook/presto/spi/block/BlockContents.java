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

import io.airlift.slice.Slice;
import static java.lang.System.arraycopy;

public class BlockContents {
    public long[] longs;
    public double[] doubles;
    public boolean[] valueIsNull;
    public Slice slice;
    public int[] offsets;
    public int[] rowNumberMap;
    int arrayOffset;
    public int positionCount;
    public boolean isIdentityMap;
    boolean isMapOwned;

    static int[] identityMap;

    static int[] getIdentityMap(int size, int start, MapHolder mapHolder)
    {
	if (start == 0) {
	    int[] map = identityMap;
	    if (map != null && map.length >= size) {
		return map;
	    }
	    map = new int[size + 100];
	    for (int i = 0; i < map.length; ++i) {
		map[i] = i;
	    }
	    identityMap = map;
	    return map;
	}
	int[] map = mapHolder.getIntArray(size);
	for (int i = 0; i < size; ++i) {
	    map[i] = i + start;
	}
	return map;
    }

    public void decodeBlock(Block block, MapHolder mapHolder) {
	int positionCount = block.getPositionCount();
	isMapOwned = false;
	isIdentityMap = true;
        longs = null;
        slice = null;
        offsets = null;
        rowNumberMap = null;
	int[] map = null;
	for (;;) {
	    if (block instanceof DictionaryBlock)
		{
		    int[] ids = ((DictionaryBlock)block).getIdsArray();
		    int offset = ((DictionaryBlock)block).getIdsOffset();
			if (map == null) {
			    map = ids;
			    if (offset != 0) {
                              int[] newMap = mapHolder.getIntArray(positionCount);
                              System.arraycopy(map, offset, newMap, 0, positionCount);
				isMapOwned = true;
				map = newMap;
			    }
			}
			else {
			    if (!isMapOwned) {
				int[] newMap = mapHolder.getIntArray(positionCount);
				for (int i = 0; i < positionCount; ++i) {
				    newMap[i] = ids[map[i] + offset];
				}
				isMapOwned = true;
			    }
			    else {
				for (int i = 0; i < positionCount; ++i) {
				    map[i] = ids[map[i] + offset];
				}
			    }
			}
		    block = ((DictionaryBlock)block).getDictionary();
		}
	    else if (block instanceof RunLengthEncodedBlock)
		{
		if (map == null || !isMapOwned) {
	    map = mapHolder.getIntArray(positionCount);
	    isMapOwned = true;
	    isIdentityMap = false;
	}
    for (int i = 0; i < positionCount; ++i) {
	map[i] = 0;
    }
    block = ((RunLengthEncodedBlock)block).getValue();
		}
	    else {
		block.getContents(this);
		if (map == null) {
                  rowNumberMap = getIdentityMap(positionCount, arrayOffset, mapHolder);
		    if (arrayOffset != 0) {
			isMapOwned = true;
			isIdentityMap = false;
		    }
		} else {
		    rowNumberMap = map;
		}
		return;
	    }
	}
    }

  public void release(MapHolder mapHolder) {
    if (isMapOwned) {
      mapHolder.store(rowNumberMap);
      isMapOwned = false;
      rowNumberMap = null;
    }
  }
	    }
