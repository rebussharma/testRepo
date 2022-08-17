FROM public.ecr.aws/lambda/python:3.8

# Copy function code
COPY Main.py ${LAMBDA_TASK_ROOT}

# Install the function's dependencies using file requirements.txt
# from your project folder.

COPY dependencies.txt  .
RUN  pip3 install -r dependencies.txt --target "${LAMBDA_TASK_ROOT}"

# Set the CMD to your handler (could also be done as a parameter override outside of the Dockerfile)
CMD [ "Main.handler" ]