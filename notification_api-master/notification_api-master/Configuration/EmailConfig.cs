using System.Collections.Generic;

namespace FFTS.IOptions
{
    public class EmailConfig
    {
        public string SMTPServer { get; set; }
        public string SenderEmail { get; set; }
        public bool ApplyWhitelist { get; set; } = true;
        public List<string> WhiteList { get; set; }      
    }

}
