import requests

def telegram_bot(token, chat_id, message):
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": message
    }
    response = requests.post(url, json=payload)
    return response.json()

def telegram_failure_notification(context):
    """Callback function for task failure."""
    task_instance = context.get('ti')
    dag_id = task_instance.dag_id
    task_id = task_instance.task_id
    execution_date = context.get('ds')
    log_url = task_instance.log_url

    message = (
        f"🚨 **Airflow Task Failed!** 🚨\n"
        f"**DAG ID:** `{dag_id}`\n"
        f"**Task ID:** `{task_id}`\n"
        f"**Execution Date:** `{execution_date}`\n"
        f"**Error Log:** [View Log]({log_url})"
    )

    telegram_bot("8477853866:AAFKrhngwull5OCbsMUaH_aia3YuCrJb5fQ", "244659756", message)

    # # Use the TelegramOperator to send the message
    # send_notification = TelegramOperator(
    #     task_id='telegram_on_failure',
    #     telegram_conn_id='telegram_notif',
        
    #     text=message,
    #     # Use HTML or Markdown for formatting
    #     parse_mode='Markdown',
    # )
    # return send_notification.execute(context=context)

def telegram_retry_notification(context):
    """Callback function for task retry."""
    task_instance = context.get('ti')
    dag_id = task_instance.dag_id
    task_id = task_instance.task_id
    try_number = task_instance.try_number
    max_tries = task_instance.max_tries
    
    # Only notify on the first or a specific retry attempt if desired
    message = (
        f"⚠️ **Airflow Task Retrying!** ⚠️\n"
        f"**DAG ID:** `{dag_id}`\n"
        f"**Task ID:** `{task_id}`\n"
        f"**Retry:** Attempt {try_number - 1} of {max_tries - 1} failed.\n"
    )

    telegram_bot("8477853866:AAFKrhngwull5OCbsMUaH_aia3YuCrJb5fQ", "244659756", message)

    # send_notification = TelegramOperator(
    #     task_id='telegram_on_retry',
    #     telegram_conn_id='telegram_notif',
    #     text=message,
    #     parse_mode='Markdown',
    # )
    # return send_notification.execute(context=context)

# telegram_bot("8477853866:AAFKrhngwull5OCbsMUaH_aia3YuCrJb5fQ", "244659756", "masuk tes")