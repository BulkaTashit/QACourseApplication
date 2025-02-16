FROM python:3.9

# Создаем рабочую директорию внутри контейнера
WORKDIR /app

# Копируем все содержимое текущей директории (где находится Dockerfile) в рабочую директорию контейнера
COPY . /app

# Устанавливаем зависимости из requirements.txt
RUN pip install -r requirements.txt

# Запускаем FastAPI приложение
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8003"]
