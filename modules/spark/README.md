#spark

##目录

###api
    模块对外接口主调用

###frame
    框架相关的操作
    
###dataset
    算子操作封装和常用混合逻辑支持

###optimal
    性能优化(注:这里只能存放性能优化的步骤,仅为了区分优化步骤使用)
    
###schema
    元数据维护
        
###data_lineage
    血缘关系/调用链

###functions
    基础udf,udtf,udaf函数和管理(单独列出是为了和市面上基础指标的相似性做研究和比较)

###structure
    数据结构相关操作
    
###component
    spark相关组件使用的备份,注意:这里仅仅为最简单的标准支持,复杂具体组件操作还是在adapter
    
###common
    公共实现操作,不能分类的就暂时存放在这里,注意只能将不能分类的操作放在此处


###examples
    案例集合(和框架相关的操作,和业务无关)
####session[案例]
SparkSession和相关算子相关的样例使用
####streaming[案例]
SparkStreaming相关的流跑批样例使用
####struct_stream[案例]
SparkStructureStreaming相关的结构流跑批(真微处理)使用
####structure[案例]
Spark的数据结构相关样例操作
