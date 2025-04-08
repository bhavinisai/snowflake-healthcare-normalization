from data_processor import DataProcessor


def main():
    input_file = "data/legacy_healthcare_data.csv"
    processor = DataProcessor(input_file)
    processor.process_all()
    processor.close()


if __name__ == "__main__":
    main()
