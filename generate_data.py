from faker import Faker
import csv
import random

LOCAL_CSV_FILE = "/home/imam/data_engineer/cloud_pipeline/employee.csv"

def generate_fake_data():
    fake = Faker()
    with open(LOCAL_CSV_FILE, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['first_name','last_name','job','company','salary','email','phone','city','country'])
        for _ in range(10000):
            writer.writerow([fake.first_name(), fake.last_name(), fake.job(), fake.company(),
                             random.randint(25, 150) * 100, fake.free_email(), fake.phone_number(),
                             fake.city(), fake.country()])

if __name__ == "__main__":
    generate_fake_data()
