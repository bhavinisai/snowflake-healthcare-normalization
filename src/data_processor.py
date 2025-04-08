# importing necessary libraries and modules
import os
import glob
import shutil
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    lit,
    to_date,
    to_timestamp,
    when,
    max as spark_max,
    date_format,
    row_number,
)
from pyspark.sql.window import Window


class DataProcessor:

    # creating Spark session and reading the input file
    def __init__(self, input_file):
        self.spark = SparkSession.builder.appName(
            "HealthcareDataNormalization"
        ).getOrCreate()
        self.input_file = input_file
        self.df = (
            self.spark.read.option("header", "true")
            .option("delimiter", ",")
            .csv(input_file)
        )
        self.output_folder = "output"  # output directory
        if not os.path.exists(self.output_folder):
            os.makedirs(self.output_folder)

    # saving to CSV function
    def save_to_csv(self, df, output_filename):
        output_path = os.path.join(self.output_folder, output_filename)
        temp_dir = output_path + "_temp"  # temporary directory for saving CSV
        df.coalesce(1).write.option("header", "true").mode("overwrite").csv(temp_dir)
        csv_files = glob.glob(os.path.join(temp_dir, "part-*.csv"))
        if not csv_files:
            print("No CSV part file found in", temp_dir)
            return
        csv_file = csv_files[0]
        shutil.move(csv_file, output_path)
        shutil.rmtree(temp_dir)  # removing temporary directory

    # 1. DimPatient
    def dim_patient(self):
        visits_df = self.df.select(  # extracting only patientID and visit date
            "patient_id",
            to_date(to_timestamp("visit_datetime", "yyyy-MM-dd HH:mm:ss.SSSSSS")).alias(
                "visit_date"
            ),
        )
        last_visit_df = visits_df.groupBy("patient_id").agg(
            spark_max("visit_date").alias("last_visit_date")
        )  # getting the last visit date for each patient
        patient_df = self.df.select(
            "patient_id",
            "patient_first_name",
            "patient_last_name",
            "patient_date_of_birth",
            "patient_gender",
            "patient_address_line1",
            "patient_address_line2",
            "patient_city",
            "patient_state",
            "patient_zip",
            "patient_phone",
            "patient_email",
        ).dropDuplicates(
            ["patient_id"]
        )  # dropping duplicates based on patient_id
        patient_df = patient_df.join(last_visit_df, on="patient_id", how="left")
        patient_df = patient_df.withColumn(
            "patient_status",
            when(col("last_visit_date") > lit("2021-12-31"), "Active").otherwise(
                "Inactive"
            ),  # if not visited after 2021-12-31, set to Inactive
        )
        dim_patient_df = patient_df.select(
            "patient_id",
            "patient_first_name",
            "patient_last_name",
            "patient_date_of_birth",
            "patient_gender",
            "patient_address_line1",
            "patient_address_line2",
            "patient_city",
            "patient_state",
            "patient_zip",
            "patient_phone",
            "patient_email",
            "patient_status",
        )  # selecting relevant columns
        self.save_to_csv(dim_patient_df, "DimPatient.csv")

    # 2. DimInsurance
    def dim_insurance(self):
        dim_insurance_df = (
            self.df.select(  # selecting relevant columns of insurance details
                "insurance_id",
                "patient_id",
                "insurance_payer_name",
                "insurance_policy_number",
                "insurance_group_number",
                "insurance_plan_type",
            ).dropDuplicates(["insurance_id", "patient_id"])
        )  # must be unique for each patient
        self.save_to_csv(dim_insurance_df, "DimInsurance.csv")

    # 3. DimBilling
    def dim_billing(self):
        dim_billing_df = self.df.select(
            "billing_id",
            "insurance_id",  # insurance_id is a foreign key
            col("billing_total_charge")
            .cast("decimal(10,2)")
            .alias("billing_total_charge"),  # total charge for the visit
            col("billing_amount_paid")
            .cast("decimal(10,2)")
            .alias("billing_amount_paid"),
            to_timestamp(col("billing_date"), "yyyy-MM-dd HH:mm:ss.SSSSSS").alias(
                "billing_ts"
            ),
            "billing_payment_status",
        ).dropDuplicates()  # removing duplicates based on billing_id

        dim_billing_df = dim_billing_df.select(
            "billing_id",
            "insurance_id",
            "billing_total_charge",
            "billing_amount_paid",
            date_format(col("billing_ts"), "yyyy-MM-dd HH:mm:ss").alias("billing_date"),
            "billing_payment_status",
        )
        self.save_to_csv(dim_billing_df, "DimBilling.csv")

    # 4. DimProvider
    def dim_provider(self):
        dim_provider_df = self.df.select(
            "doctor_name", "doctor_title", "doctor_department"
        ).dropDuplicates()  # selecting relevant columns and removing duplicates
        window_spec = Window.orderBy("doctor_name")
        dim_provider_df = dim_provider_df.withColumn(
            "provider_id",
            row_number()
            .over(window_spec)
            .cast("long"),  # creating an auto-incrementing provider_id
        ).select(
            "provider_id", "doctor_name", "doctor_title", "doctor_department"
        )  # reordering columns
        self.save_to_csv(dim_provider_df, "DimProvider.csv")

    # 5. DimLocation
    def dim_location(self):
        dim_location_df = self.df.select(
            "clinic_name", "room_number"
        ).dropDuplicates()  # selecting relevant columns and removing duplicates
        window_spec = Window.orderBy("clinic_name")  # ordering by clinic_name
        dim_location_df = dim_location_df.withColumn(
            "location_id",
            row_number()
            .over(window_spec)
            .cast("long"),  # creating an auto-incrementing location_id
        ).select(
            "location_id", "clinic_name", "room_number"
        )  # reordering columns
        self.save_to_csv(dim_location_df, "DimLocation.csv")

    # 6. DimPrimaryDiagnosis
    def dim_primary_diagnosis(self):
        dim_primary_diag_df = self.df.select(
            "primary_diagnosis_code", "primary_diagnosis_desc"
        ).dropDuplicates()  # selecting relevant columns and removing duplicates
        window_spec = Window.orderBy("primary_diagnosis_code")
        dim_primary_diag_df = dim_primary_diag_df.withColumn(
            "primary_diagnosis_id",
            row_number()
            .over(window_spec)
            .cast("long"),  # creating an auto-incrementing primary_diagnosis_id
        ).select(
            "primary_diagnosis_id", "primary_diagnosis_code", "primary_diagnosis_desc"
        )  # reordering columns
        self.save_to_csv(dim_primary_diag_df, "DimPrimaryDiagnosis.csv")

    # 7. DimSecondaryDiagnosis
    def dim_secondary_diagnosis(self):
        dim_secondary_diag_df = (
            self.df.select(
                "secondary_diagnosis_code", "secondary_diagnosis_desc"
            )  # selecting relevant columns
            .filter(
                (col("secondary_diagnosis_code").isNotNull())
                & (col("secondary_diagnosis_desc").isNotNull())
                & (col("secondary_diagnosis_code") != "")
                & (col("secondary_diagnosis_desc") != "")
            )
            .dropDuplicates()  # removing null values and duplicates
        )
        window_spec = Window.orderBy("secondary_diagnosis_code")
        dim_secondary_diag_df = dim_secondary_diag_df.withColumn(
            "secondary_diagnosis_id",
            row_number()
            .over(window_spec)
            .cast("long"),  # creating an auto-incrementing secondary_diagnosis_id
        ).select(
            "secondary_diagnosis_id",
            "secondary_diagnosis_code",
            "secondary_diagnosis_desc",
        )
        self.save_to_csv(dim_secondary_diag_df, "DimSecondaryDiagnosis.csv")

    # 8. DimTreatment
    def dim_treatment(self):
        dim_treatment_df = self.df.select(
            "treatment_code", "treatment_desc"
        ).dropDuplicates()
        window_spec = Window.orderBy("treatment_code")  # ordering by treatment_code
        dim_treatment_df = dim_treatment_df.withColumn(
            "treatment_id",
            row_number()
            .over(window_spec)
            .cast(
                "long"
            ),  # creating an auto-incrementing treatment_id using row_number()
        )
        dim_treatment_df = dim_treatment_df.select(  # reordering columns
            "treatment_id", "treatment_code", "treatment_desc"
        )
        self.save_to_csv(dim_treatment_df, "DimTreatment.csv")

    # 9. DimPrescription
    def dim_prescription(self):
        dim_prescription_df = self.df.select(  # selecting relevant columns
            "prescription_id",
            "prescription_drug_name",
            "prescription_dosage",
            "prescription_frequency",
            col("prescription_duration_days")
            .cast("int")
            .alias("prescription_duration_days"),
        ).filter(
            col("prescription_id").isNotNull()  # removing null values
        )
        dim_prescription_df = dim_prescription_df.dropDuplicates(
            ["prescription_id"]
        )  # removing duplicates based on prescription_id
        self.save_to_csv(dim_prescription_df, "DimPrescription.csv")

    # 10. DimLabOrder
    def dim_lab_order(self):
        dim_lab_order_df = self.df.select(
            "lab_order_id",
            "lab_test_code",
            "lab_name",
            "lab_result_value",
            "lab_result_units",
            "lab_result_date",
        ).filter(
            (col("lab_order_id").isNotNull()) & (col("lab_order_id") != "")
        )  # removing null values

        dim_lab_order_df = (
            dim_lab_order_df.withColumn(
                "lab_result_ts", to_timestamp("lab_result_date")
            )
            .withColumn(
                "lab_result_date", date_format("lab_result_ts", "yyyy-MM-dd HH:mm:ss")
            )
            .drop("lab_result_ts")
        )

        dim_lab_order_df = dim_lab_order_df.dropDuplicates(
            ["lab_order_id"]
        )  # removing duplicates based on lab_order_id
        self.save_to_csv(dim_lab_order_df, "DimLabOrder.csv")

    # 11. FactVisit

    def fact_visit(self):
        # Step 1: Select base visit data
        fact_df = self.df.select(
            "visit_id",
            "patient_id",
            "insurance_id",
            "billing_id",
            "doctor_name",
            "doctor_title",
            "doctor_department",
            "clinic_name",
            "room_number",
            "primary_diagnosis_code",
            "primary_diagnosis_desc",
            "secondary_diagnosis_code",
            "secondary_diagnosis_desc",
            "treatment_code",
            "treatment_desc",
            "prescription_id",
            "lab_order_id",
            to_timestamp("visit_datetime", "yyyy-MM-dd HH:mm:ss.SSSSSS").alias(
                "visit_ts"
            ),
            "visit_type",
        ).dropDuplicates(["visit_id"])

        # creating window specifications for row_number()
        w_provider = Window.orderBy("doctor_name")
        w_location = Window.orderBy("clinic_name")
        w_primary = Window.orderBy("primary_diagnosis_code")
        w_secondary = Window.orderBy("secondary_diagnosis_code")
        w_treatment = Window.orderBy("treatment_code")

        # joining provider_id
        provider_df = self.df.select(
            "doctor_name", "doctor_title", "doctor_department"
        ).dropDuplicates()
        provider_df = provider_df.withColumn(
            "provider_id", row_number().over(w_provider).cast("long")
        )
        fact_df = fact_df.join(
            provider_df,
            on=["doctor_name", "doctor_title", "doctor_department"],
            how="left",
        )

        # joining location_id
        location_df = self.df.select("clinic_name", "room_number").dropDuplicates()
        location_df = location_df.withColumn(
            "location_id", row_number().over(w_location).cast("long")
        )
        fact_df = fact_df.join(
            location_df, on=["clinic_name", "room_number"], how="left"
        )

        # joining primary_diagnosis_id
        primary_df = self.df.select(
            "primary_diagnosis_code", "primary_diagnosis_desc"
        ).dropDuplicates()
        primary_df = primary_df.withColumn(
            "primary_diagnosis_id", row_number().over(w_primary).cast("long")
        )
        fact_df = fact_df.join(
            primary_df,
            on=["primary_diagnosis_code", "primary_diagnosis_desc"],
            how="left",
        )

        # joining secondary_diagnosis_id
        secondary_df = self.df.select(
            "secondary_diagnosis_code", "secondary_diagnosis_desc"
        ).dropDuplicates()
        secondary_df = secondary_df.withColumn(
            "secondary_diagnosis_id", row_number().over(w_secondary).cast("long")
        )
        fact_df = fact_df.join(
            secondary_df,
            on=["secondary_diagnosis_code", "secondary_diagnosis_desc"],
            how="left",
        )

        # joining treatment_id
        treatment_df = self.df.select(
            "treatment_code", "treatment_desc"
        ).dropDuplicates()
        treatment_df = treatment_df.withColumn(
            "treatment_id", row_number().over(w_treatment).cast("long")
        )
        fact_df = fact_df.join(
            treatment_df, on=["treatment_code", "treatment_desc"], how="left"
        )

        # selecting relevant columns for fact visit table
        fact_visit_df = fact_df.select(
            "visit_id",
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
            date_format("visit_ts", "yyyy-MM-dd HH:mm:ss").alias("visit_datetime"),
            "visit_type",
        )
        self.save_to_csv(fact_visit_df, "FactVisit.csv")

    # processing all tables
    def process_all(self):
        self.dim_patient()
        self.dim_insurance()
        self.dim_billing()
        self.dim_provider()
        self.dim_location()
        self.dim_primary_diagnosis()
        self.dim_secondary_diagnosis()
        self.dim_treatment()
        self.dim_prescription()
        self.dim_lab_order()
        self.fact_visit()
        print("All the tables are processed and saved to CSV files!")

    def close(self):
        self.spark.stop()
