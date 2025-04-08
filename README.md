# Legacy Healthcare Data Normalization using PySpark and Tableau

## Project Overview

This project focuses on transforming a legacy healthcare dataset into a normalized Snowflake Schema using PySpark. The goal is to clean, structure, and export dimension and fact tables that can support advanced analytics and visualization in Tableau. This transformation improves both data quality and usability by eliminating redundancy, ensuring consistency and creating structured relationships between different healthcare entities. 

The workflow includes:
- Normalizing a flat file into dimension and fact tables
- Creating surrogate keys for efficient lookups
- Cleaning data, removing duplicates, and formatting timestamps
- Saving the normalized tables as individual CSV files

---

## Dataset Description 

The legacy dataset used in this project is a flat CSV file containing a mix of clinical, administrative, and operational healthcare data. It contains:

### Patient Details
- `patient_id`, `patient_first_name`, `patient_last_name`
- `patient_date_of_birth`, `patient_gender`
- Full address, phone, and email

### Visit Information
- `visit_id`, `visit_datetime`, `visit_type`

### Insurance
- `insurance_id`, `insurance_payer_name`
- `insurance_policy_number`, `insurance_group_number`
- `insurance_plan_type`

### Billing
- `billing_id`, `billing_total_charge`, `billing_amount_paid`
- `billing_date`, `billing_payment_status`

### Provider & Location
- `doctor_name`, `doctor_title`, `doctor_department`
- `clinic_name`, `room_number`

### Diagnoses
- `primary_diagnosis_code`, `primary_diagnosis_desc`
- `secondary_diagnosis_code`, `secondary_diagnosis_desc`

### Prescriptions
- `prescription_id`, `prescription_drug_name`
- `prescription_dosage`, `prescription_frequency`, `prescription_duration_days`

### Lab Orders
- `lab_order_id`, `lab_test_code`, `lab_name`
- `lab_result_value`, `lab_result_units`, `lab_result_date`

The dataset may contain duplicate or null values, or inconsistencies and is pre-processed during normalization.

---

## Schema Used 

This project implements a **Snowflake Schema**, consisting of: 

### Dimension Tables: 
- `DimPatient`
- `DimInsurance`
- `DimBilling`
- `DimProvider`
- `DimLocation`
- `DimPrimaryDiagnosis`
- `DimSecondaryDiagnosis`
- `DimTreatment`
- `DimPrescription`
- `DimLabOrder`

### Fact Table: 
- `FactVisit` — references all dimension tables via foreign keys

---

## Required Packages

- Python 3.x
- PySpark  
- Standard libraries: `os`, `glob`, `shutil`

Install PySpark:
```bash
pip install pyspark
```

---

## Execution

1. Place the raw CSV file (`legacy_healthcare_data.csv`) inside the project folder.

2. Ensure that the script is located at `src/main.py`.

3. Run the following command from the root directory of the project:
```bash
python3 src/main.py
```

The script will
- initialize a Spark session
- read and normalize the raw data
- create 10 dimension tables and a FactVisit table
- export each table into a CSV file in the `output/` directory

![Snowflake Schema](snowflake/healthcare_diagram.webp)

---

## Output Files

The following files which have fully normalized tables will be generated after processing: 

- `DimBilling.csv`
- `DimInsurance.csv`
- `DimLabOrder.csv`
- `DimLocation.csv`
- `DimPatient.csv`
- `DimPrescription.csv`
- `DimPrimaryDiagnosis.csv`
- `DimProvider.csv`
- `DimSecondaryDiagnosis.csv`
- `DimTreatment.csv`
- `FactVisit.csv`

---

## Data Validation

To ensure the correctness of the normalized data:
- Verified that all primary keys in dimension tables are referenced as foreign keys in the FactVisit table.
- Checked for duplicate and null values in critical columns before assigning surrogate keys.
- Performed sample joins between fact and dimension tables using PySpark to confirm referential integrity.
- Validated the final CSV outputs by loading them into Tableau for consistent aggregation and filtering.

---

## Tableau Visualization

**[Legacy Healthcare Dashboard](https://public.tableau.com/app/profile/bhavini.sai.mallu/viz/LegacyHealthcare-DSCI644Project3/Dashboard1)**

- **Monthly Visit Trends** – Tracks seasonal patient flow
- **Clinic & Visit Type Distribution** – Treemap showing load by clinic and visit category
- **Department Visits by Gender** – Gender breakdown across departments
- **Billing Accumulation** – Area chart of monthly billing totals
- **Doctor Activity** – Visit counts by doctor and department

---

## Author

**Bhavini Sai Mallu**  
Graduate Student
Rochester Institute of Technology  
bm5726@g.rit.edu

This project was completed as part of the DSCI-644 course: Software Engineering for Data Science, with a focus on healthcare data normalization and visualization using PySpark and Tableau.

---
