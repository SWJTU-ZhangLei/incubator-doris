package src.main.java.org.apache.doris.catalog;

import java.io.DataOutputStream;
import java.util.Map;

import org.apache.doris.persist.gson.GsonUtils;

public class DroppedColumnNameManager {

    // tableId/indexId -> AddDropColumnHistory
    private Map<long, DroppedColumnName> tableDropppedColumnNames = new Map<<long, DroppedColumnName>();

    // ref to Catalog::saveDb
    public long write(DataOutputStream dos, long checksum) {
        // write size
        int tableCount = tableDropppedColumnNames.size();
        checksum ^= tableCount;
        dos.writeInt(tableCount);
        for (Map.Entry<long, DroppedColumnName> entry : tableDropppedColumnNames.entrySet()) {
            DroppedColumnName droppedColumnName = entry.getValue();
            checksum ^= entry.getKey();
            dos.writeLong(entry.getKey());
            db.write(dos);
        }
        return checksum;
    }

     public void read() {
        // according to write
     }

     class DroppedColumnName { // including rename col, rename == drop + add
        private String[] colNames;
        private void write(DataOutputStream dos) {
            dos.write(GSON.toJson(this));
        }
        private void write(DataOutputStream dos) {
            dos.write(GSON.toJson(this));
        }
    };
}
