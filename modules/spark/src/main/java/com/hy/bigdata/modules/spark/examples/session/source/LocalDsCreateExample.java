package com.hy.bigdata.modules.spark.examples.session.source;

import com.hy.bigdata.modules.spark.api.SparkSessionTemplate;
import org.apache.commons.collections.CollectionUtils;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

/**
 * @author yang.he
 * @date 2021-04-15 10:53
 *
 *  本地dataset制造案例
 *
 */
public class LocalDsCreateExample {

    final private static SparkSessionTemplate SESSION_TEMPLATE = SparkSessionTemplate.getInstance();
    final private static SparkSession SESSION = SESSION_TEMPLATE.getSession();
    final private static JavaSparkContext CONTEXT = SESSION_TEMPLATE.getContext();

    public static void main(String[] args) {
        createByStructTypeExample().show();
    }


    /**
     * 快速创建字符schema制造的Dataset
     *
     * @param lines     行数据
     * @param colNames  每个行对应的字符名
     * @return  快速创建字符schema制造的Dataset
     */
    public static Dataset<Row> quickCreateStrDs(List<Row> lines,List<String> colNames) {
        if (CollectionUtils.isEmpty(lines)||CollectionUtils.isEmpty(colNames)){
            throw new RuntimeException("创建的行数据或者对应的列名为空");
        }

        List<StructField> struct = new ArrayList<>();
        for (String colName:colNames){
            struct.add(DataTypes.createStructField(colName, DataTypes.StringType, true));
        }
        StructType schema = DataTypes.createStructType(struct);

        return SESSION.createDataFrame(lines, schema);
    }


    /**
     * @return  返回通过StructType制造的Dataset
     */
    public static Dataset<Row> createByStructTypeExample() {

        List<Row> list = new ArrayList<>();
        list.add(new GenericRow(new Object[]{1,"you","test,ww,test"}));
        list.add(new GenericRow(new Object[]{2,"me","test,yy,mm"}));
        list.add(new GenericRow(new Object[]{2,"her",null}));

        List<StructField> schemaList = new ArrayList<>();
        schemaList.add(DataTypes.createStructField("id", DataTypes.IntegerType, true));
        schemaList.add(DataTypes.createStructField("name", DataTypes.StringType, true));
        schemaList.add(DataTypes.createStructField("content", DataTypes.StringType, true));

        StructType schema = DataTypes.createStructType(schemaList);

        return SESSION.createDataFrame(list, schema);
    }


    /**
     * @return  返回通过kryo序列化Bean制造的Dataset
     */
    public static Dataset<DsDataBean> createBeanByEncoderExample() {
        List<DsDataBean> dataBeans = new ArrayList<>();
        dataBeans.add(new DsDataBean(1,"he","man"));
        dataBeans.add(new DsDataBean(2,"her","woman"));
        return SESSION.createDataset(dataBeans, Encoders.kryo(DsDataBean.class));
    }



    public static class DsDataBean{
        private Integer id;
        private String name;
        private String content;

        public Integer getId() {
            return id;
        }

        public void setId(Integer id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getContent() {
            return content;
        }

        public void setContent(String content) {
            this.content = content;
        }

        public DsDataBean(Integer id, String name, String content) {
            this.id = id;
            this.name = name;
            this.content = content;
        }

        @Override
        public String toString() {
            return "DsDataBean{" +
                    "id=" + id +
                    ", name='" + name + '\'' +
                    ", content='" + content + '\'' +
                    '}';
        }
    }

}
