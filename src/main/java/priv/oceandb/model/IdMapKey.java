package priv.oceandb.model;

import lombok.AllArgsConstructor;

import java.util.Arrays;

/**
 * id表映射的key，供cache使用
 * rowkey family qualifier
 * family: 0->id 1->name
 * qualifier: 0->param 1->prop_key 2->prop_value
 */
@AllArgsConstructor
public class IdMapKey {

    private byte[] rowkey;
    private int family;
    private int quafier;

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        IdMapKey key = (IdMapKey) o;
        return Arrays.equals(this.rowkey, key.rowkey) &&
                this.family == key.family &&
                this.quafier == key.quafier;
    }

    @Override
    public int hashCode() {
        // 尽量避免哈希冲突
        // 31*i -> (i<<5)-i
        return 31 * (31 * Arrays.hashCode(rowkey) + family) + quafier;
    }

}
