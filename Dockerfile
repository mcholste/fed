FROM python:2-onbuild

# Create app directory
RUN mkdir -p /opt/fed
WORKDIR /opt/fed/

# Create conf directory
RUN mkdir -p /opt/fed/conf

# Bundle app source
COPY . /opt/fed
ADD config.json /opt/fed/conf/config.json

EXPOSE 8888

CMD ["python", "/opt/fed/fed/app.py", "/opt/fed/conf/config.json"]
