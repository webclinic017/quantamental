import time
import smtplib
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import COMMASPACE

from spac.config import Secret,Config

def send_email(subject, body, filenames, path_to_file ):
    try:

        server = smtplib.SMTP("smtp.gmail.com", "587")
        server.starttls()
        server.login(Secret.EMAILFROM,Secret.EMAILPW)

        message = MIMEMultipart()
        message['From'] = Secret.EMAILFROM
        message['Bcc'] = COMMASPACE.join(Secret.EMAILDICT['bcc'])
        message['Subject'] = subject

        # Add body to email
        message.attach(MIMEText(body, "plain"))

        for filename in filenames:
            file_full_name = path_to_file + filename
            with open(file_full_name, "rb") as attachment:
                # Add file as application/octet-stream
                # Email client can usually download this automatically as attachment
                part = MIMEApplication(
                    attachment.read(),
                    Name=filename
                )

            # Add header as key/value pair to attachment part
            part['Content-Disposition'] = 'attachment; filename="%s"' % (filename)

            # Add attachment to message and convert message to string
            message.attach(part)
        text = message.as_string()

        sent = 0
        retry = 0
        while sent == 0:
            try:
                server.sendmail(Secret.EMAILFROM, Secret.EMAILDICT['bcc'], text)
                print('sent email ')
                sent += 1
            except Exception as e:
                time.sleep(1)  # TODO not good
                retry += 1
                if retry == 2:
                    raise Exception("Failed to send Email after retry %s" % e)
                    break
    except Exception as e:
        print('Send email Exception', e)
    finally:
        server.quit()