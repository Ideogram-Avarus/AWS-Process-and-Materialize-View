FROM public.ecr.aws/lambda/python:3.12

# Copia o diretório src e o arquivo requirements.txt para o container
COPY src/ ${LAMBDA_TASK_ROOT}/src/
COPY requirements.txt ${LAMBDA_TASK_ROOT}/

RUN pip install --no-cache-dir --target "${LAMBDA_TASK_ROOT}" -r ${LAMBDA_TASK_ROOT}/requirements.txt

# Adicione o diretório src ao PYTHONPATH
ENV PYTHONPATH="${LAMBDA_TASK_ROOT}/src"

CMD ["src/lambda_function.lambda_handler"]