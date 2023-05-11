from processor.dataprocessor_service import DataProcessorService


"""
    Main-модуль, т.е. модуль запуска приложений ("точка входа" приложения)
"""


if __name__ == '__main__':
    service = DataProcessorService(datasource="SYB65_328_202209_Intentional_homicides_and_other_crimes_1.csv", db_connection_url="sqlite:///main_db.db")
    service.run_service()
