/*
 * Copyright (c) 2014, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */

package com.cloudera.oryx.als.serving;

import java.util.Iterator;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Floats;

import com.cloudera.oryx.als.common.NumericIDValue;
import com.cloudera.oryx.common.collection.LongObjectMap;
import com.cloudera.oryx.common.collection.LongSet;
import com.cloudera.oryx.common.iterator.LongPrimitiveIterator;
import com.cloudera.oryx.common.math.SimpleVectorMath;

/**
 * An {@link Iterator} that generates and iterates over all possible most surprising items.
 *
 * @author Sean Owen
 * @see com.cloudera.oryx.als.serving.RecommendIterator
 */
final class MostSurprisingIterator implements Iterator<NumericIDValue> {

  private final NumericIDValue delegate;
  private final float[] features;
  private final LongPrimitiveIterator knownItemIDs;
  private final LongObjectMap<float[]> Y;

  MostSurprisingIterator(float[] features,
                         LongSet knownItemIDs,
                         LongObjectMap<float[]> Y) {
    delegate = new NumericIDValue();
    this.features = features;
    this.knownItemIDs = knownItemIDs.iterator();
    this.Y = Y;
  }

  @Override
  public boolean hasNext() {
    return knownItemIDs.hasNext();
  }

  @Override
  public NumericIDValue next() {
    long itemID = knownItemIDs.nextLong();
    float[] itemFeatures = Y.get(itemID);
    float result = (float) (1.0 - SimpleVectorMath.dot(itemFeatures, features));
    Preconditions.checkState(Floats.isFinite(result), "Bad recommendation value");
    delegate.set(itemID, result);
    return delegate;
  }

  /**
   * @throws UnsupportedOperationException
   */
  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }

}
