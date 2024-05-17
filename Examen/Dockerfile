#Imagen base ubuntu 20.04
FROM ubuntu:20.04

#Configurar entorno
#ENV = variables de entorno
#RUN = comando de construccion
#Zona horaria de ubuntu
ENV TZ=America/New_York
#Configuramos la zona horaria del contenedor
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone
#Actualizamos todos los apquetes del SO a las ultimas versiones disponibles
RUN apt-get update && apt-get upgrade -y
#Instalar paquetes necesarios para una app web como python y pip
RUN apt-get install -y python3 python3-pip curl
#Instalar Flask
RUN pip3 install flask
#Crear el directorio /app
RUN mkdir /multi
#Establecer /app como el directorio de trabajo para poder trabajar con los comandos del Dockerfile como RUN, COPY, WORKDIR, FROM, CMD, ENTRYPOINT
WORKDIR /multi
# Copiar el código de la aplicación al directorio /app
COPY . /multi
#Exponer el puerto que la app usara
EXPOSE 5000
#Definir el comando para ejecutar la app
CMD ["python3", "multi.py"]

