import logging
import os
import smtplib
from email.message import EmailMessage


log = logging.getLogger("email_client")


def _parse_bool_env(name: str, default: bool) -> bool:
    raw_value = os.getenv(name)
    if raw_value is None:
        return default
    return raw_value.strip().lower() in {"1", "true", "yes", "on"}


class EmailClient:
    def __init__(
        self,
        host: str,
        port: int,
        from_address: str,
        to_addresses: list[str],
        username: str = "",
        password: str = "",
        use_tls: bool = True,
        use_ssl: bool | None = None,
    ):
        self.host = (host or "").strip()
        self.port = int(port)
        self.from_address = (from_address or "").strip()
        self.to_addresses = [addr.strip() for addr in to_addresses if addr and addr.strip()]
        self.username = (username or "").strip()
        self.password = password or ""
        self.use_tls = bool(use_tls)
        self.use_ssl = _parse_bool_env("SMTP_USE_SSL", False) if use_ssl is None else bool(use_ssl)

        if not self.host:
            raise ValueError("SMTP_HOST not configured")
        if not self.port:
            raise ValueError("SMTP_PORT not configured")
        if not self.from_address:
            raise ValueError("SMTP_FROM not configured")
        if not self.to_addresses:
            raise ValueError("ALERT_EMAIL_TO not configured")
        if self.password and not self.username:
            raise ValueError("SMTP_USERNAME is required when SMTP_PASSWORD is set")

    def send_message(self, subject: str, text: str) -> None:
        message = EmailMessage()
        message["Subject"] = subject
        message["From"] = self.from_address
        message["To"] = ", ".join(self.to_addresses)
        message.set_content(text)

        if self.use_ssl:
            smtp_factory = smtplib.SMTP_SSL
        else:
            smtp_factory = smtplib.SMTP

        with smtp_factory(self.host, self.port, timeout=15) as smtp:
            if not self.use_ssl:
                smtp.ehlo()
                if self.use_tls:
                    smtp.starttls()
                    smtp.ehlo()
            if self.username:
                smtp.login(self.username, self.password)
            smtp.send_message(message)

        log.info(
            "Email alert sent from=%s to=%s subject=%s use_ssl=%s use_tls=%s",
            self.from_address,
            ",".join(self.to_addresses),
            subject,
            self.use_ssl,
            self.use_tls,
        )
