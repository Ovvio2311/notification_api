
using Microsoft.Extensions.Configuration;
using System;
using System.Threading.Tasks;
using System.Net.Http;
using Newtonsoft.Json;
using FFTS.InternalCommonModels.DBModels;
using System.Net.Http.Json;
using FFTS.InternalCommonModels.DBModels.ACCMGT.Account;
using FFTS.InternalCommonModels.APIRequestObjects.ShortLinkAPI;
using FFTS.InternalCommonModels.DBModels.ACCMGT.Payment;
using System.Collections.Generic;

namespace notification_api.Services
{
    public class PaymentAndBillingService
    {
        private readonly IConfiguration _configuration;
        private HttpClient _httpClient;
        private readonly int HttpClientTimeOut;


        public PaymentAndBillingService(IConfiguration configuration)
        {
            _configuration = configuration;

            _httpClient = new();
            _httpClient.BaseAddress = new Uri(_configuration["PaymentAndBillingAPIHost"]);

            HttpClientTimeOut = _configuration.GetValue<int>("HttpClientTimeOut");
            _httpClient.Timeout = TimeSpan.FromSeconds(HttpClientTimeOut);
        }

        public async Task<MakeRVOSevElevShortLinkResponse> GetRVOSevElevShortLink(long cust_tran_id)
        {
            var model = new { cust_trxn_id = cust_tran_id};
            string requestURI = "/PaymentAndBilling/MakeRVOSevElevShortLink";
            HttpResponseMessage res = await _httpClient.PostAsync(requestURI, JsonContent.Create(model));

            res.EnsureSuccessStatusCode();

            var result = JsonConvert.DeserializeObject<MakeRVOSevElevShortLinkResponse>(res.Content.ReadAsStringAsync().Result);

            return result;
        }

        public async Task<ViewPaymentTokenResponse> ViewPaymentTokenList(long acct_id)
        {
            var model = new { acct_id = acct_id };
            string requestURI = "/PaymentAndBilling/ViewPaymentTokenList";
            HttpResponseMessage res = await _httpClient.PostAsync(requestURI, JsonContent.Create(model));

            res.EnsureSuccessStatusCode();

            var result = JsonConvert.DeserializeObject<ViewPaymentTokenResponse>(res.Content.ReadAsStringAsync().Result);

            return result;
        }
    }

    public class MakeRVOSevElevShortLinkResponse : PSPBaseResponseObject
    {
        public string short_link_val { get; set; }
    }

    public class PSPBaseResponseObject
    {
        public bool result { get; set; } = false;
        public string message { get; set; }
        public int result_code { get; set; }
    }

    public class ViewPaymentTokenResponse
    {
        public List<tb_auto_payment_token> auto_payment_token_list { get; set; }
    }
}
