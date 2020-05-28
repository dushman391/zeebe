/*
 * Copyright © 2020  camunda services GmbH (info@camunda.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package io.atomix.raft.snapshot;

import io.atomix.utils.time.WallClockTimestamp;
import io.zeebe.util.CloseableSilently;
import java.io.IOException;
import java.util.Optional;

public interface SnapshotStore extends CloseableSilently {

  /**
   * Returns true if a snapshot with the given ID exists already, false otherwise.
   *
   * @param id the snapshot ID to look for
   * @return true if there is a committed snapshot with this ID, false otherwise
   */
  boolean exists(String id);

  // for replication
  TransientSnapshot takeTransientSnapshot(
      final long index, final long term, final WallClockTimestamp timestamp);

  TransientSnapshot takeTransientSnapshot(String snapshotId);

  Optional<Snapshot> getLatestSnapshot();

  void purgePendingSnapshots() throws IOException;

  void addSnapshotListener(SnapshotListener listener);

  void removeSnapshotListener(SnapshotListener listener);

  long getCurrentSnapshotIndex();

  /**
   * Deletes a {@link SnapshotStore} from disk.
   *
   * <p>The snapshot store will be deleted by simply reading {@code snapshot} file names from disk
   * and deleting snapshot files directly. Deleting the snapshot store does not involve reading any
   * snapshot files into memory.
   */
  void delete();
}
