def get_month_start_n_months_ago(n):
  return (datetime.now() - timedelta(days=n)).replace(day=1).strftime