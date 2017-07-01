FROM python:3

RUN pip install jupyter
RUN pip install boto3
RUN pip install arrow
RUN pip install pandas 

RUN mkdir /Assignment1

WORKDIR /Assignment1

EXPOSE 8888

ADD *.py *.json /Assignment1/
ADD cleanDataEDA.ipynb /Assignment1/
#ADD run.sh /Assignment1/


#RUN chmod 755 /Assignment1/run.sh
RUN chmod 755 /Assignment1/wrangle.py
RUN chmod 755 /Assignment1
RUN chmod 755 /Assignment1/*

CMD ["python", "wrangle.py"]




