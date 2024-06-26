from setuptools import setup,find_packages

setup(
    name='MDBtoPG', 
    version='1.0',              
    packages=find_packages(),  
    install_requires=[          
        'pymongo',
        'psycopg2',
    ],
    author='Keyler Sanchez',       
    author_email='keylersanchez00@gmail.com',
    description='Conversor de bases de datos de Mongo a Postgres',
    url='https://url-de-tu-paquete.com')