FROM python:3.9-bullseye
ADD lambda_kinesis_ConvertZipFiles.py .
RUN pip install pandas
RUN pip install boto3
RUN pip install requests
#run the application
CMD ["python", "./lambda_kinesis_ConvertZipFiles.py"]


 