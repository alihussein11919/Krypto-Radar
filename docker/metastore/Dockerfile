FROM apache/hive:4.0.0

# Add the Postgres JDBC driver which the base image is missing
ADD https://jdbc.postgresql.org/download/postgresql-42.7.3.jar /opt/hive/lib/postgresql-42.7.3.jar

USER root
RUN chmod 644 /opt/hive/lib/postgresql-42.7.3.jar
USER hive
