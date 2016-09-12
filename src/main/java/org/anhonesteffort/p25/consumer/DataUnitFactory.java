/*
 * Copyright (C) 2016 An Honest Effort LLC.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.anhonesteffort.p25.consumer;

import org.anhonesteffort.p25.protocol.Duid;
import org.anhonesteffort.p25.protocol.Nid;
import org.anhonesteffort.p25.protocol.frame.DataUnit;
import org.anhonesteffort.p25.protocol.frame.HeaderDataUnit;
import org.anhonesteffort.p25.protocol.frame.LinkControlWordTerminatorDataUnit;
import org.anhonesteffort.p25.protocol.frame.LogicalLinkDataUnit1;
import org.anhonesteffort.p25.protocol.frame.LogicalLinkDataUnit2;
import org.anhonesteffort.p25.protocol.frame.SimpleTerminatorDataUnit;
import org.anhonesteffort.p25.protocol.frame.TrunkSignalDataUnit;

import java.nio.ByteBuffer;

import static org.anhonesteffort.kinesis.proto.ProtoP25.P25DataUnit;

public class DataUnitFactory {

  private Nid nid(P25DataUnit.Reader dataUnit) {
    return new Nid(dataUnit.getNac(), new Duid(dataUnit.getDuid()), true);
  }

  public DataUnit create(P25DataUnit.Reader dataUnit) {
    Nid        nid    = nid(dataUnit);
    ByteBuffer buffer = ByteBuffer.wrap(dataUnit.getBytes().toArray());

    switch (nid.getDuid().getId()) {
      case Duid.ID_HEADER:
        return new HeaderDataUnit(nid, buffer);

      case Duid.ID_TERMINATOR_WO_LINK:
        return new SimpleTerminatorDataUnit(nid, buffer);

      case Duid.ID_LLDU1:
        return new LogicalLinkDataUnit1(nid, buffer);

      case Duid.ID_TRUNK_SIGNALING:
        return new TrunkSignalDataUnit(nid, buffer);

      case Duid.ID_LLDU2:
        return new LogicalLinkDataUnit2(nid, buffer);

      case Duid.ID_TERMINATOR_W_LINK:
        return new LinkControlWordTerminatorDataUnit(nid, buffer);

      default:
        return new DataUnit(nid, buffer);
    }
  }

}
