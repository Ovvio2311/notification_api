namespace FFTS.IOptions
{
    public class SendNotification
    {
        public string PulsarUri { get; set; }
        public string tlsCertPath { get; set; }
        public string tlsCertFile { get; set; }
        public Topics Topics { get; set; }
        public SendTopics SendTopics { get; set; }
        public SkipPulsar SkipPulsar { get; set; }
        public int sendTimeoutSecond { get; set; } = 30;
        public bool Enable { get; set; } = true;
        public bool EnablePulsarClientLogger { get; set; } = false;
    }

    public class Topics
    {
        public string HighPriority { get; set; }
        public string MediumPriority { get; set; }
        public string LowPriority { get; set; }
    }

    public class SendTopics
    {
        public email email { get; set; }
        public sms sms { get; set; }
        public apps apps { get; set; }
    }

    public class SkipPulsar
    {
        public toemail toemail { get; set; }
        public tosms tosms { get; set; }
        public toapps toapps { get; set; }
    }
    public class email
    {
        public string high { get; set; }
        public string medium { get; set; }
        public string low { get; set; }
    }
    public class sms
    {
        public string high { get; set; }
        public string medium { get; set; }
        public string low { get; set; }
    }
    public class apps
    {
        public string high { get; set; }
        public string medium { get; set; }
        public string low { get; set; }
    }

    public class toemail
    {
        public bool high { get; set; } = false;
        public bool medium { get; set; } = false;
        public bool low { get; set; } = false;
    }
    public class tosms
    {
        public bool high { get; set; } = false;
        public bool medium { get; set; } = false;
        public bool low { get; set; } = false;
    }
    public class toapps
    {
        public bool high { get; set; } = false;
        public bool medium { get; set; } = false;
        public bool low { get; set; } = false;
    }
}
