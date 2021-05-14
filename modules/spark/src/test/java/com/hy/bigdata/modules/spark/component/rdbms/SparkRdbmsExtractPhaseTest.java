package com.hy.bigdata.modules.spark.component.rdbms;

import org.junit.Test;

/**
 * @author yang.he
 * @date 2021-05-12 17:13
 */
public class SparkRdbmsExtractPhaseTest {

    @Test
    public void getRdbmsTable() {
        long aml_country = SparkRdbmsExtractPhase.getRdbmsTable("aml_country", null).count();
        System.out.println(aml_country);
    }
}