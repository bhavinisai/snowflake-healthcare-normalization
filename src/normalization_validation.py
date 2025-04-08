from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# setting base path for the output files
base_path = "output"

# initialize Spark session
spark = SparkSession.builder.appName("NormalizationValidation").getOrCreate()

# loading all tables
DimPatient = spark.read.option("header", "true").csv(f"{base_path}/DimPatient.csv")
DimInsurance = spark.read.option("header", "true").csv(f"{base_path}/DimInsurance.csv")
DimBilling = spark.read.option("header", "true").csv(f"{base_path}/DimBilling.csv")
DimProvider = spark.read.option("header", "true").csv(f"{base_path}/DimProvider.csv")
DimLocation = spark.read.option("header", "true").csv(f"{base_path}/DimLocation.csv")
DimPrimaryDiagnosis = spark.read.option("header", "true").csv(
    f"{base_path}/DimPrimaryDiagnosis.csv"
)
DimSecondaryDiagnosis = spark.read.option("header", "true").csv(
    f"{base_path}/DimSecondaryDiagnosis.csv"
)
DimTreatment = spark.read.option("header", "true").csv(f"{base_path}/DimTreatment.csv")
DimPrescription = spark.read.option("header", "true").csv(
    f"{base_path}/DimPrescription.csv"
)
DimLabOrder = spark.read.option("header", "true").csv(f"{base_path}/DimLabOrder.csv")
FactVisit = spark.read.option("header", "true").csv(f"{base_path}/FactVisit.csv")

# checking for null values in primary keys
print("\n Checking for NULLs in primary keys...")
DimPatient.select("patient_id").filter(col("patient_id").isNull()).show()
DimInsurance.select("insurance_id").filter(col("insurance_id").isNull()).show()
DimBilling.select("billing_id").filter(col("billing_id").isNull()).show()
DimProvider.select("provider_id").filter(col("provider_id").isNull()).show()
DimLocation.select("location_id").filter(col("location_id").isNull()).show()
DimPrimaryDiagnosis.select("primary_diagnosis_id").filter(
    col("primary_diagnosis_id").isNull()
).show()
DimSecondaryDiagnosis.select("secondary_diagnosis_id").filter(
    col("secondary_diagnosis_id").isNull()
).show()
DimTreatment.select("treatment_id").filter(col("treatment_id").isNull()).show()
DimPrescription.select("prescription_id").filter(col("prescription_id").isNull()).show()
DimLabOrder.select("lab_order_id").filter(col("lab_order_id").isNull()).show()
FactVisit.select("visit_id").filter(col("visit_id").isNull()).show()

# checking if FactVisit foreign keys exist in corresponding dimension tables
print("\n Validating foreign key references from FactVisit...")


def check_fk(fact_df, fk_col, dim_df, dim_col, name):
    missing = fact_df.select(fk_col).subtract(dim_df.select(dim_col))
    count_missing = missing.count()
    print(f"{name}: {count_missing} missing keys")
    if count_missing > 0:
        missing.show()


check_fk(FactVisit, "patient_id", DimPatient, "patient_id", "patient_id")
check_fk(FactVisit, "insurance_id", DimInsurance, "insurance_id", "insurance_id")
check_fk(FactVisit, "billing_id", DimBilling, "billing_id", "billing_id")
check_fk(FactVisit, "provider_id", DimProvider, "provider_id", "provider_id")
check_fk(FactVisit, "location_id", DimLocation, "location_id", "location_id")
check_fk(
    FactVisit,
    "primary_diagnosis_id",
    DimPrimaryDiagnosis,
    "primary_diagnosis_id",
    "primary_diagnosis_id",
)
check_fk(
    FactVisit,
    "secondary_diagnosis_id",
    DimSecondaryDiagnosis,
    "secondary_diagnosis_id",
    "secondary_diagnosis_id",
)
check_fk(FactVisit, "treatment_id", DimTreatment, "treatment_id", "treatment_id")
check_fk(
    FactVisit, "prescription_id", DimPrescription, "prescription_id", "prescription_id"
)
check_fk(FactVisit, "lab_order_id", DimLabOrder, "lab_order_id", "lab_order_id")

# null values check for foreign keys in FactVisit
print("\n Checking for NULLs in FactVisit foreign keys...")
fk_columns = [
    "patient_id",
    "insurance_id",
    "billing_id",
    "provider_id",
    "location_id",
    "primary_diagnosis_id",
    "secondary_diagnosis_id",
    "treatment_id",
    "prescription_id",
    "lab_order_id",
]
for fk in fk_columns:
    FactVisit.filter(col(fk).isNull()).select("visit_id", fk).show()

# checking patient status logic
print("\n Verifying patient_status column in DimPatient...")
DimPatient.groupBy("patient_status").count().show()

# printing the row counts for each table
print("\n Row counts for all tables:")
for name, df in [
    ("DimPatient", DimPatient),
    ("DimInsurance", DimInsurance),
    ("DimBilling", DimBilling),
    ("DimProvider", DimProvider),
    ("DimLocation", DimLocation),
    ("DimPrimaryDiagnosis", DimPrimaryDiagnosis),
    ("DimSecondaryDiagnosis", DimSecondaryDiagnosis),
    ("DimTreatment", DimTreatment),
    ("DimPrescription", DimPrescription),
    ("DimLabOrder", DimLabOrder),
    ("FactVisit", FactVisit),
]:
    print(f"{name}: {df.count()} rows")

spark.stop()
print("\nValidation completed.")
