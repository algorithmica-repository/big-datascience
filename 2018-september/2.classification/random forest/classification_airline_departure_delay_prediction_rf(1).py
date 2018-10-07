# Databricks notebook source
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler, OneHotEncoderEstimator
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from collections import defaultdict
from pyspark.ml.feature import Imputer
from pyspark.sql.functions import col,sum
import datetime
from pyspark.ml import PipelineModel

# COMMAND ----------

def get_types(df):
    print(df.schema)
    print(df.schema.fields)
    data_types_map = defaultdict(list)
    for entry in df.schema.fields:
      data_types_map[str(entry.dataType)].append(entry.name)
    print(data_types_map)
    return data_types_map

def filter_rows_with_missing_labels(df, target):
    #print(df.count())
    tmp = df.filter(df[target].isNotNull())
    print(tmp.count())
    return tmp
  
def drop_features(df, features):
    print(len(df.columns))
    for feature in features:
      df = df.drop(feature)
    print(len(df.columns))
    return df

def filter_features_with_missing_data(df):
    tmp = df.filter(df[target].isNotNull())
    return tmp
  
def get_missing_info(df):
    tmp = df.select(*(sum(col(c).isNull().cast("int")).alias(c) for c in df.columns))
    display(tmp)

def eda_categorical_feature(df, feature):
    tmp = df.groupby(feature).count().toPandas()
    print(tmp)
        
def eda_continuous_features(df, feature):
    df.describe(feature).show()
  
def cast_integer_features_as_double(df, data_types_map):
    for c in data_types_map["IntegerType"]:
       df = df.withColumn(c, df[c].cast('double'))
    df.printSchema()
    return df
        
def imputer_continuous_features(df, data_types_map):
    continuous_features = list(set(data_types_map['DoubleType']) - set(['DEP_DEL15']))
    continuous_features_imputed = [var + "_imputed" for var in  continuous_features]
    imputer = Imputer(inputCols = continuous_features, outputCols = continuous_features_imputed)
    tmp = imputer.fit(df).transform(df)
    get_missing_info(tmp)
    return [imputer]

def impute_categorical_features(df, data_types_map):
    missing_data_fill = {}
    for var in data_types_map['StringType']:
      missing_data_fill[var] = "missing"
    tmp = df.fillna(missing_data_fill)
    get_missing_info(tmp)
    return tmp
    
def encoder_categorical_features(df, data_types_map):
    categorical_features = list(set(data_types_map['StringType']) - set(['_c46']))
    string_indexers = [StringIndexer(inputCol=feature, outputCol=feature+"_index") for feature in categorical_features]

    ohe_input_features = [ feature+"_index" for feature in categorical_features]
    ohe_output_features = [ feature+"_encoded" for feature in categorical_features]
    ohe_encoder = OneHotEncoderEstimator(inputCols=ohe_input_features, outputCols=ohe_output_features)  
    pipeline = Pipeline(stages=string_indexers + [ohe_encoder])
    tmp = pipeline.fit(df).transform(df)
    tmp.printSchema()
    return string_indexers + [ohe_encoder]

def assembler_for_feature_vector(df, data_types_map):
    features = []
    #categorical_features = list(set(data_types_map['StringType']) - set(['_c46']))
    #features = features + [ feature+"_encoded" for feature in categorical_features]
    
    continuous_features = list(set(data_types_map['DoubleType']) - set(['DEP_DEL15']))
    features = features + [var + "_imputed" for var in  continuous_features]
    print(features)
    assembler = VectorAssembler(inputCols= features, outputCol="features")
    #tmp = assembler.transform(df)
    #tmp.printSchema()
    return [assembler]

def split_data(df, train_percent):
    return df.randomSplit([train_percent, 1-train_percent], seed=123);

def build_and_tune_model_with_cv(estimator, param_grid, evaluatior, df):
    # define grid based cross validator
    crossval = CrossValidator(estimator=pipeline,
                          estimatorParamMaps=param_grid,
                          evaluator= evaluator,
                          numFolds=2)

    # train model using fit
    cv_model = crossval.fit(df)
    return cv_model
  
def model_summary_rf(cv_model):
    print(cv_model)
    print(cv_model.avgMetrics)
    best_model = cv_model.bestModel
    print(best_model)
    print(best_model.stages)
    rf_model = best_model.stages[-1]
    print(rf_model)
    print(rf_model.featureImportances)
    print(rf_model.trees)
    print(rf_model.treeWeights)
    for tree in rf_model.trees:
      print(tree.toDebugString)
  
def predict(cv_model, evaluator, df):
    # evaluate model on test set
    predictions = cv_model.transform(df)
    print(predictions)
    predictions.printSchema()
    print(evaluator.evaluate(predictions))

def persist_model(cv_model, type, model_dir):
    datestamp = datetime.datetime.now().strftime('%m-%d-%Y-%s');
    file_name = "cv_model_" + type + "_" + datestamp
    path = model_dir + file_name
    print(path)
    cv_model.bestModel.save(path)
    
def load_model(path):
    return PipelineModel.load(path)



# COMMAND ----------

airline_delays = spark.read.load("/FileStore/tables/airline_delays.csv",
                             format="csv", header="true", inferSchema="true", sep=",")
airline_delays.printSchema()


# COMMAND ----------

display(airline_delays)

# COMMAND ----------

print(airline_delays.count())
data_types_map = get_types(airline_delays)

# COMMAND ----------

airline_delays = cast_integer_features_as_double(airline_delays, data_types_map)

# COMMAND ----------

airline_delays.printSchema()

# COMMAND ----------

target = 'DEP_DEL15'
eda_categorical_feature(airline_delays, target)

# COMMAND ----------

airline_delays_not_null = filter_rows_with_missing_labels(airline_delays, target)


# COMMAND ----------

eda_categorical_feature(airline_delays_not_null, target)

# COMMAND ----------

#split the data into train and validation sets
airline_delays_train, airline_delays_test = split_data(airline_delays_not_null, 0.75)

#persist the train and test frames
airline_delays_train.persist(); 
print(airline_delays_train.count());
airline_delays_test.persist(); 
print(airline_delays_test.count());


# COMMAND ----------

get_missing_info(airline_delays_train)

# COMMAND ----------

#impute categorical features
airline_delays_train = impute_categorical_features(airline_delays_train, data_types_map)

# COMMAND ----------

#imputer for continuous features
imputer = imputer_continuous_features(airline_delays_train, data_types_map)

# COMMAND ----------

#encoder for categorical features
encoder = encoder_categorical_features(airline_delays_train, data_types_map)

# COMMAND ----------

#assembler for vector data
assembler = assembler_for_feature_vector(airline_delays_train, data_types_map)

# COMMAND ----------

pipeline = Pipeline(stages=imputer + encoder + assembler)
tmp = pipeline.fit(airline_delays_train).transform(airline_delays_train)
tmp.printSchema()

# COMMAND ----------

#define the estimator
randForest = RandomForestClassifier(featuresCol='features', labelCol = target)

# define the modeling pipeline with formula + feature transofrmations + estimator
pipeline = Pipeline(stages=imputer + encoder + assembler + [randForest])

#define binary classification evaluator with right metric
evaluator = BinaryClassificationEvaluator(labelCol=target, metricName="areaUnderROC")

# Define the parameter grid for random forest
param_grid = ParamGridBuilder() \
    .addGrid(randForest.numTrees, [10]) \
    .addGrid(randForest.maxDepth, [3]) \
    .build()

cv_model = build_and_tune_model_with_cv(pipeline, param_grid, evaluator, airline_delays_train)


# COMMAND ----------

model_summary_rf(cv_model)

# COMMAND ----------

predict(cv_model, evaluator, airline_delays_test)

# COMMAND ----------

persist_model(cv_model, "rf", "/FileStore/tables/")

# COMMAND ----------

loaded_model = load_model("/FileStore/tables/CV_Model_rf_10-06-2018-1538862687")
print(loaded_model)
predict(loaded_model, evaluator, airline_delays_test)
