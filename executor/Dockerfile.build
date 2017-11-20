FROM python:3.5

RUN pip install pyinstaller==3.3

RUN mkdir /opt/cook
WORKDIR /opt/cook

COPY requirements.txt /opt/cook/
RUN pip install -r requirements.txt

COPY . /opt/cook

# Create a one-folder bundle containing an executable (instead of using the one-file version).
# Allows us to avoid the extraction to a temporary folder needed by the PyInstaller bootloader.
CMD ["pyinstaller", "--onedir", "--name", "cook-executor", "--paths", "cook", "cook/__main__.py"]
