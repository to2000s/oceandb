package priv.oceandb.service;

import org.springframework.lang.Nullable;
import priv.oceandb.model.DataPoint;

import java.io.IOException;

public interface ReadService {

    DataPoint query(String param, long timestamp, double lat, double lng, String[] props) throws IOException;

    DataPoint[] scan(String param, long[] timestamp, @Nullable double[] latLng, @Nullable String[] props);
}
