using Orleans;

namespace OrleansCassandraUtils.Reminders
{
    public class CassandraReminderTableOptions
    {
        [RedactConnectionString]
        public string ConnectionString { get; set; }
    }
}
