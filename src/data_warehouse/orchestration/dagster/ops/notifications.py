"""
Notification operations for Dagster.

This module provides operations for sending notifications about pipeline execution status.
"""

from dagster import OpExecutionContext, op


@op(
    description="Sends a success notification",
    config_schema={
        "recipients": str,
        "subject": str,
    },
)
def send_success_notification(context: OpExecutionContext) -> None:
    """
    Send a notification when a pipeline succeeds.

    Args:
        context: The Dagster execution context
    """
    recipients = context.op_config["recipients"]
    subject = context.op_config["subject"]

    # In a real implementation, this would send an email, Slack message, etc.
    context.log.info(f"SUCCESS NOTIFICATION (would send to {recipients}): {subject}")

    # Example code for sending an email (commented out)
    # import smtplib
    # from email.message import EmailMessage
    #
    # msg = EmailMessage()
    # msg.set_content(f"Pipeline succeeded: {context.run_id}")
    # msg['Subject'] = subject
    # msg['From'] = "dagster@example.com"
    # msg['To'] = recipients
    #
    # server = smtplib.SMTP('smtp.example.com', 587)
    # server.send_message(msg)
    # server.quit()


@op(
    description="Sends a failure notification",
    config_schema={
        "recipients": str,
        "subject": str,
    },
)
def send_failure_notification(context: OpExecutionContext, error_messages: list[str] | None = None) -> None:
    """
    Send a notification when a pipeline fails.

    Args:
        context: The Dagster execution context
        error_messages: Optional list of error messages to include in the notification
    """
    recipients = context.op_config["recipients"]
    subject = context.op_config["subject"]

    error_detail = ""
    if error_messages:
        error_detail = "\nErrors:\n" + "\n".join(f"- {msg}" for msg in error_messages)

    # In a real implementation, this would send an email, Slack message, etc.
    context.log.error(f"FAILURE NOTIFICATION (would send to {recipients}): {subject}{error_detail}")

    # Example code for sending an email (commented out)
    # import smtplib
    # from email.message import EmailMessage
    #
    # msg = EmailMessage()
    # msg.set_content(f"Pipeline failed: {context.run_id}{error_detail}")
    # msg['Subject'] = subject
    # msg['From'] = "dagster@example.com"
    # msg['To'] = recipients
    #
    # server = smtplib.SMTP('smtp.example.com', 587)
    # server.send_message(msg)
    # server.quit()
