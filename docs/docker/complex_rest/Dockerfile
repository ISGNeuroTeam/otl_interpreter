FROM python:3.9.7
ENV PYTHONUNBUFFERED=1

ENV DATABASE=postgres
ENV DB_USER=postgres
ENV DB_PASSWORD=postgres
ENV DB_HOST=postgres
ENV DB_PORT=5432
ENV REST_CONF=/complex_rest/docs/docker_dev/complex_rest/rest.conf
ENV PYTHONPATH=/complex_rest/complex_rest

RUN mkdir /complex_rest

ADD ./docs/docker/complex_rest/id_rsa_plugin /root/.ssh/id_rsa_plugin
RUN chmod 600 /root/.ssh/id_rsa_plugin
RUN ssh-keyscan -t rsa github.com >> /root/.ssh/known_hosts
RUN eval "$(ssh-agent -s)" && ssh-add /root/.ssh/id_rsa_plugin && git clone git@github.com:ISGNeuroTeam/complex_rest.git

RUN chmod -R 0777 /complex_rest
WORKDIR /complex_rest

RUN pip install --no-cache-dir  -r requirements.txt


ENTRYPOINT ["/complex_rest/docs/docker_dev/complex_rest/entrypoint.sh"]
CMD ["python /complex_rest/complex_rest/manage.py runserver [::]:8080"]
EXPOSE 8080