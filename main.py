"""
Created on Tue Apr 3 17:50:50 2025

@author: fanGam
"""

from sqlalchemy import create_engine, text
import os
import sys
import pandas as pd

def create_db():
    file_path = "config.txt"

    if os.path.exists(file_path):
        with open(file_path, "r") as f:
            try:
                connection_string = f.readline()
                database_name = f.readline()
            except Exception as e:
                print("Error with config file. Do you want to continiue without ? (y/n)")
                choice = input("Your choice: ")
                if choice.lower() == "y":
                    try:
                        user_pass = input("User:Password: ")
                        server = input("Server/name: ")
                        connection_string = (
                            f"mssql+pyodbc://{user_pass}@{server}"
                            "?driver=ODBC+Driver+17+for+SQL+Server"
                        )
                    except Exception as ex:
                        print(f"Error with : {ex}")
                        sys.exit(1)
                if choice.lower == "n":
                    print("User cancelled.")
                    sys.exit(1)
                else:
                    print("Invalid choice. Exiting.")
                    sys.exit(1)

    try:
        engine = create_engine(connection_string)
        with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as connection:       
            result = connection.execute(text("SELECT name FROM sys.databases"))
            databases = result.fetchall()
            names = []
            
            # print("Список баз данных:")
            for db in databases:
                names.append(db[0])
            print(names)
            
            # Обработка если база данных с таким названием уже существует
            if database_name in names:
                print(f"База данных {database_name} уже существует.\n"
                    "1 - удалить старую и создать новую на её месте\n"
                    "2 - создать новую с другим названием\n"
                    "3 - завершить сеанс")
                choice = input("Введите номер: ")
                
                if choice == '1':
                    connection.execute(text(f"""
                        USE master; 
                        ALTER DATABASE {database_name} SET SINGLE_USER WITH ROLLBACK IMMEDIATE; 
                        DROP DATABASE {database_name};"""
                    ))
                    print('test dropped')
                elif choice == '2':
                    database_name = input("Введите новое имя базы: ")
                elif choice == '3':
                    raise Exception('User cancelled')
                else:
                    raise Exception('User input error')
            
            # Создание базы данных
            connection.execute(text(f"CREATE DATABASE {database_name};"))
            connection.execute(text(f"USE {database_name};"))
            connection.execute(text(f"""
                CREATE TABLE test (
                    user_id INT,
                    question_id INT,
                    answer INT,
                    answer_group INT,
                    answer_time DATETIME NOT NULL,
                    PRIMARY KEY (user_id, question_id)
                );
            """))
            print("База данных создана!")
            
            print("Желаете ли создать файл с конфигурацией для дальнейшей работы ? (y/n)")
            choice = input("Your choice: ")
            if choice.lower() == 'y':
                with open("config.txt", 'w') as f:
                    f.write(connection_string)
                    f.write(database_name)
                print("Config файл создан")
            else:
                print("При следующем запуске снова введите данные")
    except Exception as e:
        print("Ошибка подключения к базе данных:", e)
        
def make_answer(user_id, question_id, answer, answer_group):
    file_path = "config.txt"

    if os.path.exists(file_path):
        with open(file_path, "r") as f:
            try:
                connection_string = f.readline()
                database_name = f.readline()
            except Exception as e:
                print("Error with config file. Do you want to continiue without ? (y/n)")
                choice = input("Your choice: ")
                if choice.lower() == "y":
                    try:
                        user_pass = input("User:Password: ")
                        server = input("Server/name: ")
                        connection_string = (
                            f"mssql+pyodbc://{user_pass}@{server}"
                            "?driver=ODBC+Driver+17+for+SQL+Server"
                        )
                    except Exception as ex:
                        print(f"Error with : {ex}")
                        sys.exit(1)
                if choice.lower == "n":
                    print("User cancelled.")
                    sys.exit(1)
                else:
                    print("Invalid choice. Exiting.")
                    sys.exit(1)
    else:
        print("Заполните файл config.txt в формате: \nconnection_string\ndatabase_name\n"
            "или создайте файл через создание базы данных")
        sys.exit(1)
        
    try:
        engine = create_engine(connection_string)
        with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as connection:
            connection.execute(text(f"USE {database_name};"))
            connection.execute(text(f"""
                INSERT INTO test (user_id, question_id, answer, answer_group, answer_time)
                VALUES ({user_id}, {question_id}, {answer}, {answer_group}, GETDATE())
            """))
    except Exception as e:
        print("Ошибка подключения к базе данных:", e)
        
def stats():
    file_path = "config.txt"

    if os.path.exists(file_path):
        with open(file_path, "r") as f:
            try:
                connection_string = f.readline()
                database_name = f.readline()
            except Exception as e:
                print("Error with config file. Do you want to continiue without ? (y/n)")
                choice = input("Your choice: ")
                if choice.lower() == "y":
                    try:
                        user_pass = input("User:Password: ")
                        server = input("Server/name: ")
                        connection_string = (
                            f"mssql+pyodbc://{user_pass}@{server}"
                            "?driver=ODBC+Driver+17+for+SQL+Server"
                        )
                    except Exception as ex:
                        print(f"Error with : {ex}")
                        sys.exit(1)
                if choice.lower == "n":
                    print("User cancelled.")
                    sys.exit(1)
                else:
                    print("Invalid choice. Exiting.")
                    sys.exit(1)
    else:
        print("Заполните файл config.txt в формате: \nconnection_string\ndatabase_name\n"
            "или создайте файл через создание базы данных")
        sys.exit(1)
        
    try:
        engine = create_engine(connection_string)
        with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as connection:
            connection.execute(text(f"USE {database_name};"))
            data = connection.execute(text("SELECT * FROM test"))
            df = pd.DataFrame(data.fetchall(), columns=data.keys())
            # print(df.head())
            # Заполнение правильных ответов (за них принимаются ответы, где user_id == 1 (можно изменить при необходимости))
            correct_ans = df[df["user_id"] == 1].set_index("question_id")["answer"]
            # print(correct_ans)
            df["result"] = df.apply(
                lambda x : x["answer"] == correct_ans[x["question_id"]], axis=1
            )
            # print(df.head())
            correct_counts = (
                df.groupby(["user_id", "answer_group"])["result"]
                .sum()
                .astype(int)
                .reset_index(name="correct_by_group")
            )
            print(correct_counts)
    except Exception as e:
        print("Ошибка подключения к базе данных:", e)
        
def result_by_user():
    file_path = "config.txt"
    
    min_scores = {
        "1" : 1,
        "2" : 1
    }
    

    if os.path.exists(file_path):
        with open(file_path, "r") as f:
            try:
                connection_string = f.readline()
                database_name = f.readline()
            except Exception as e:
                print("Error with config file. Do you want to continiue without ? (y/n)")
                choice = input("Your choice: ")
                if choice.lower() == "y":
                    try:
                        user_pass = input("User:Password: ")
                        server = input("Server/name: ")
                        connection_string = (
                            f"mssql+pyodbc://{user_pass}@{server}"
                            "?driver=ODBC+Driver+17+for+SQL+Server"
                        )
                    except Exception as ex:
                        print(f"Error with : {ex}")
                        sys.exit(1)
                if choice.lower == "n":
                    print("User cancelled.")
                    sys.exit(1)
                else:
                    print("Invalid choice. Exiting.")
                    sys.exit(1)
    else:
        print("Заполните файл config.txt в формате: \nconnection_string\ndatabase_name\n"
            "или создайте файл через создание базы данных")
        sys.exit(1)
        
    try:
        engine = create_engine(connection_string)
        with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as connection:
            connection.execute(text(f"USE {database_name};"))
            data = connection.execute(text("SELECT * FROM test"))
            df = pd.DataFrame(data.fetchall(), columns=data.keys())
            # print(df.head())
            # Заполнение правильных ответов (за них принимаются ответы, где user_id == 1 (можно изменить при необходимости))
            correct_ans = df[df["user_id"] == 1].set_index("question_id")["answer"]
            # print(correct_ans)
            df["result"] = df.apply(
                lambda x : x["answer"] == correct_ans[x["question_id"]], axis=1
            )
            # print(df.head())
            correct_counts = (
                df.groupby(["user_id", "answer_group"])["result"]
                .sum()
                .astype(int)
                .reset_index(name="correct_by_group")
            )
            
            # Заполнение минимальных баллов (за них принимаются ответы, где user_id == 1 (можно изменить при необходимости))
            min_scores = (
                correct_counts[correct_counts["user_id"] == 1].
                set_index("answer_group")["correct_by_group"].
                mul(0.75).
                rename("min_scores").
                reset_index()
            )
            # print(min_scores)
            result_users = correct_counts.merge(min_scores, on="answer_group")
            
            result_users["is_passed"] = result_users["correct_by_group"] >= result_users["min_scores"]
            
            # вывод результата для каждого пользователя
            for user_id in result_users["user_id"].unique():
                user_data = result_users[result_users["user_id"] == user_id]
                for group in user_data["answer_group"].unique():
                    if user_data[user_data["answer_group"] == group]["is_passed"].iloc[0]:
                        print(f"User {user_id} done well with {group}")
                    else:
                        print(f"User {user_id} failed with {group}")
            
            
    except Exception as e:
        print("Ошибка подключения к базе данных:", e)