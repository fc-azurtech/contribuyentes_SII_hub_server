import smtplib
from email.mime.text import MIMEText


class NotificationService:
    def __init__(self, smtp_host, smtp_port, smtp_user, smtp_password, smtp_tls, sender, target):
        self.smtp_host = smtp_host
        self.smtp_port = smtp_port
        self.smtp_user = smtp_user
        self.smtp_password = smtp_password
        self.smtp_tls = smtp_tls
        self.sender = sender
        self.target = target

    def send_failure_email(self, subject: str, body: str) -> None:
        if not self.smtp_host or not self.target:
            return

        msg = MIMEText(body, "plain", "utf-8")
        msg["Subject"] = subject
        msg["From"] = self.sender
        msg["To"] = self.target

        with smtplib.SMTP(self.smtp_host, self.smtp_port, timeout=20) as smtp:
            if self.smtp_tls:
                smtp.starttls()
            if self.smtp_user:
                smtp.login(self.smtp_user, self.smtp_password)
            smtp.sendmail(self.sender, [self.target], msg.as_string())
