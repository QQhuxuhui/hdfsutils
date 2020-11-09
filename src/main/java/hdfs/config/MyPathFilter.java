package hdfs.config;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapred.FileInputFormat;

import java.util.ArrayList;
import java.util.List;

public class MyPathFilter {
    private static final PathFilter hiddenFileFilter = new PathFilter() {
        public boolean accept(Path p) {
            String name = p.getName();
            return !name.startsWith("_") && !name.startsWith(".");
        }
    };

    public static PathFilter hiddenFileFilter(){
        List<PathFilter> filters = new ArrayList();
        filters.add(hiddenFileFilter);
        PathFilter inputFilter = new MultiPathFilter(filters);
        return inputFilter;
    }

    private static class MultiPathFilter implements PathFilter {
        private List<PathFilter> filters;

        public MultiPathFilter(List<PathFilter> filters) {
            this.filters = filters;
        }

        public boolean accept(Path path) {
            for (PathFilter filter : filters) {
                if (!filter.accept(path)) {
                    return false;
                }
            }
            return true;
        }
    }
}
