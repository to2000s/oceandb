package priv.oceandb.service.impl;

import com.google.common.geometry.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import priv.filters.AreaFilter;
import priv.oceandb.model.DataPoint;
import priv.oceandb.service.ReadService;
import priv.oceandb.utils.CacheUtil;
import priv.oceandb.utils.DecodeUtil;
import priv.oceandb.utils.EncodeUtil;
import priv.oceandb.dao.DataDao;
import priv.oceandb.dao.IdDao;
import priv.oceandb.utils.TransferUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Service
public class ReadServiceImpl implements ReadService {
    //TODO 各种查询条件要补充
    @Override
    public List<DataPoint> scan(String param) throws IOException {
        return null;
    }

    @Override
    public List<DataPoint> scan(String[] props) throws IOException {
        return null;
    }

    @Override
    public List<DataPoint> scan(String param, long[] timestamp, String[] props) throws IOException {
        return null;
    }

    @Override
    public List<DataPoint> scan(String param, double[] latLng, String[] props) throws IOException {
        return null;
    }

    @Override
    public List<DataPoint> scan(String param, long[] timestamp, double[] latLng, String[] props) throws IOException {
        return null;
    }

    @Autowired
    EncodeUtil encodeUtil;
    @Autowired
    DecodeUtil decodeUtil;
    @Autowired
    TransferUtil transferUtil;
    @Autowired
    DataDao dataDao;
    @Autowired
    IdDao idDao;
    @Autowired
    CacheUtil cacheUtil;

    @Override
    public DataPoint query(DataPoint dataPoint) throws IOException {



        dataPoint.setValue(0);
        byte[][] result = transferUtil.trans2byte(dataPoint);
        // rowkey, qualifier
        byte[] value = dataDao.query(result[0], result[1]);
        if (value != null) {
            dataPoint.setValue(decodeUtil.getValue(value));
            return dataPoint;
        }
        return null;

//        return value == null ? null : dataPoint;
//        return dataPoint;
    }

    @Override
    public List<DataPoint> scan(String param, long[] timestamp) throws IOException {

        byte[] paramId = cacheUtil.getId(param, 0, 0);
        byte[] start = Bytes.add(paramId, encodeUtil.getTimeBaseBytes(timestamp[0]));
        byte[] end = Bytes.add(paramId, encodeUtil.getTimeBaseBytes(timestamp[1]));

        List<Filter> filters = new ArrayList<>();
        // 如果在同一个小时内？
        if (Arrays.equals(start, end)) {
            filters.add(new RowFilter(CompareFilter.CompareOp.EQUAL, new BinaryPrefixComparator(start)));
        } else {
            filters.add(
                    new RowFilter(CompareFilter.CompareOp.GREATER_OR_EQUAL, new BinaryPrefixComparator(start))
            );
            filters.add(
                    new RowFilter(CompareFilter.CompareOp.LESS, new BinaryPrefixComparator(end))
            );
        }

        List<DataPoint> dataPoints = dataDao.scan(filters);
        // 寻找不满足条件的下标 求 [ ) 区间
        int startIdx = binarySearchTimeIdx(dataPoints, timestamp[0]);
//        System.out.println("startIdx: " + startIdx);
        int endIdx = binarySearchTimeIdx(dataPoints, timestamp[1]);
//        System.out.println("endIdx: " + endIdx);

        // [ )
        return dataPoints.subList(startIdx, endIdx);
    }

    @Override
    public List<DataPoint> scan(String param, double[] latLng) throws IOException {

        List<DataPoint> result = new ArrayList<>();

        byte[] paramId = cacheUtil.getId(param, 0, 0);

        List<S2Point> vertices = new ArrayList<>();
        for (int i = 0; i < latLng.length / 2; i++) {
            // 纬度在前 东经为正 北纬为正
            vertices.add(S2LatLng.fromDegrees(latLng[2 * i], latLng[2 * i + 1]).toPoint());
        }
        S2Loop s2Loop = new S2Loop(vertices);
        S2Polygon s2Polygon = new S2Polygon(s2Loop);

        // coverer 求覆盖改多边形的CellId
        S2RegionCoverer coverer = new S2RegionCoverer();
        coverer.setMaxLevel(10);
        coverer.setMinLevel(10);

        ArrayList<S2CellId> ids = coverer.getCovering(s2Polygon).cellIds();

        // 对ids取 level 10 的cellId
        List<S2CellId> s2CellIds = new ArrayList<>();
        for (S2CellId id : ids) {
            s2CellIds.addAll(childrenCellId(id, 10));
//            System.out.println(id.toToken());
        }
//        System.out.println(s2CellIds.size());

        // 对所有level 10 的cellId进行判断
        // 完全在区域内，筛选出的所有数据都符合要求
        // 不完全在区域内，进一步判断每个data point
        List<S2CellId> ids0 = new ArrayList<>();
        List<S2CellId> ids1 = new ArrayList<>();
        for (S2CellId id : s2CellIds) {

            if (s2Polygon.contains(new S2Cell(id))) {
                ids0.add(id);
            } else {
                ids1.add(id);
            }
        }

        List<Filter> passAllFilters0 = new ArrayList<>();
        List<Filter> passOneFilters0 = new ArrayList<>();
        for (S2CellId id : ids0) {
            // 每次都遍历，效率比较慢了些
            // 满足param条件，其余条件满足一个即可
            passOneFilters0.add(
                    new AreaFilter(Bytes.head(Bytes.toBytes(id.id()), 3))
            );
        }
        passAllFilters0.add(
                new RowFilter(CompareFilter.CompareOp.EQUAL, new BinaryPrefixComparator(paramId))
        );
        result.addAll(dataDao.scan(passAllFilters0, passOneFilters0));

        // 不包含的只能一个个判断了
        List<Filter> passAllFilters1 = new ArrayList<>();
        List<Filter> passOneFilters1 = new ArrayList<>();
        for (S2CellId id : ids1) {
            passOneFilters1.add(
                    new AreaFilter(Bytes.head(Bytes.toBytes(id.id()), 3))
            );
        }
        passAllFilters1.add(
                new RowFilter(CompareFilter.CompareOp.EQUAL, new BinaryPrefixComparator(paramId))
        );

        List<DataPoint> temp = dataDao.scan(passAllFilters1, passOneFilters1);
        if (temp != null) {
            for (DataPoint point : temp) {
                //判断数据点是否在区域内
                if (s2Polygon.contains(S2LatLng.fromDegrees(point.getLat(), point.getLng()).toPoint())) {
                    result.add(point);
                }
            }
        }

        return result;
    }

    /*--------------------私有方法----------------------/

    /**
     * 二分查找符合范围的data point
     * 数组上限 int 上限，20亿+，足够了，不然内存也不够
     * 选择相等或略大的一个，这样满足 [ ) 区间
     */
    private int binarySearchTimeIdx(List<DataPoint> dataPoints, long timestamp) {
        int p = 0, q = dataPoints.size() - 1;

        // 超出上、下限
        if (dataPoints.get(0).getTimestamp() > timestamp) {
            return 0;
        }
        if (dataPoints.get(q).getTimestamp() < timestamp) {
            return q + 1;
        }

        int mid;
        // 相等退出
        while (p < q) {
            mid = (p + q) / 2;
            if (dataPoints.get(mid).getTimestamp() == timestamp) {
                return mid;
            } else if (dataPoints.get(mid).getTimestamp() > timestamp) {
                q = mid;
            } else {
                p = mid + 1;
            }
        }
        // 两个相邻时，mid=p
        // mid(p)>time q=mid=p
        // mid(p)<time p=mid+1=q
        return q;
    }

    /**
     * 获取CellId指定level的子CellId
     */
    public static List<S2CellId> childrenCellId(S2CellId s2CellId, Integer desLevel) {
        return childrenCellId(s2CellId, s2CellId.level(), desLevel);
    }

    private static List<S2CellId> childrenCellId(S2CellId s2CellId, Integer curLevel, Integer desLevel) {
        if (curLevel < desLevel) {
            long interval = (s2CellId.childEnd().id() - s2CellId.childBegin().id()) / 4;
            List<S2CellId> s2CellIds = new ArrayList<S2CellId>();
            for (int i = 0; i < 4; i++) {
                long id = s2CellId.childBegin().id() + interval * i;
                s2CellIds.addAll(childrenCellId(new S2CellId(id), curLevel + 1, desLevel));
            }
            return s2CellIds;
        } else {
            List<S2CellId> s2CellIds = new ArrayList<S2CellId>();
            s2CellIds.add(s2CellId);
            return s2CellIds;
        }
    }

}
