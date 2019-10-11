package hdfs.compress;

/**
 * @Auther: huxuhui
 * @Date: 2019/10/9 13:51
 * @Description:
 */
public enum FileTypeEnum {

    SEQUENCE("sequence", 1),
    TEXT("text", 2),
    ORC("orc", 3),
    UNKNOW("unknow", 0);

    private String typeName;
    private int index;

    private FileTypeEnum(String typeName, int index) {
        this.typeName = typeName;
        this.index = index;
    }

    public String getTypeName() {
        return typeName;
    }

    public void setTypeName(String typeName) {
        this.typeName = typeName;
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

}
