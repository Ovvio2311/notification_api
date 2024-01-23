using Dapper;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using FFTS.InternalCommonDBUtils;
using FFTS.InternalCommonDBUtils.Models;
using System.Linq;
using System.Net.Http;
using Newtonsoft.Json;
using FFTS.InternalCommonModels.DBModels;
using System.Net.Http.Json;
using FFTS.InternalCommonModels.DBModels.ACCMGT.Account;

namespace notification_api.Services
{
    public class AccountSearchService
    {
        private readonly IConfiguration _configuration;
        private HttpClient _httpClient;
        private readonly int HttpClientTimeOut;


        public AccountSearchService(IConfiguration configuration)
        {
            _configuration = configuration;

            _httpClient = new();
            _httpClient.BaseAddress = new Uri(_configuration["DBServiceAccMgtAPIHost"]);

            HttpClientTimeOut = _configuration.GetValue<int>("HttpClientTimeOut");
            _httpClient.Timeout = TimeSpan.FromSeconds(HttpClientTimeOut);
        }

        public async Task<tb_acct_summary> GetAccountSummary(long? account_id, bool? is_perm)
        {
            var model = new { account_id = account_id, is_perm = is_perm };
            string requestURI = "/Account/CDB_ACCTMGT039_GetAccountSummary";
            HttpResponseMessage res = await _httpClient.PostAsync(requestURI, JsonContent.Create(model));

            res.EnsureSuccessStatusCode();

            var result = JsonConvert.DeserializeObject<DBResult>(await res.Content.ReadAsStringAsync());
            var jstring = JsonConvert.SerializeObject(result.ResultItem);

            return JsonConvert.DeserializeObject<tb_acct_summary>(jstring) ?? throw new Exception(result.result.message);
        }
	}
}
