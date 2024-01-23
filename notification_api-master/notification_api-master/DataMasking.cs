using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;

namespace notification_api
{
    public static class DataMasking
    {
        public static string nameMask(string inStr)
        {
            if (inStr == null) return null;
            if (inStr.Trim() == "") return "";
            List<string> strList = inStr.Split(" ").ToList();
            for (int i = 0; i < strList.Count; i++)
            {
                strList[i] = strList[i].Mask(1, strList[i].Length - 1, '*');
            }

            return string.Join(" ", strList);
        }

        public static string phoneNumberMask(string inStr)
        {
            if (inStr == null) return null;
            if (inStr.Length != 8) return null;

            return inStr.Mask(4, 4, '*');
        }

        public static string brNumberMask(string inStr)
        {
            if (inStr == null) return null;
            if (inStr.Length <= 4) return null;

            return inStr.Mask(0, 4, '*');
        }

        public static string emailMask(string inStr)
        {
            if (inStr == null) return null;
            if (new EmailAddressAttribute().IsValid(inStr) == false) return null;
            List<string> strList = inStr.Split("@").ToList();

            strList[0] = strList[0].Length >= 3 ? strList[0].Mask(1, strList[0].Length - 2, '*') : strList[0].Length == 2 ? strList[0].Substring(0, 1) + "*" : strList[0];
            strList[1] = strList[1].Mask(0, strList[1].IndexOf('.') == -1 ? strList[1].Length : strList[1].IndexOf('.'), '*');

            return string.Join("@", strList);
        }

        public static string addressMask(string inStr)
        {
            if (inStr == null) return null;
            if (inStr.Trim() == "") return "";
            List<string> strList = inStr.Split(",").ToList();
            for (int i = 0; i < strList.Count; i++)
            {
                strList[i] = strList[i].Trim();
                strList[i] = strList[i].Mask(1, strList[i].Length - 1, '*');
            }

            return string.Join(", ", strList);
        }

        public static string vehicleIdMask(string inStr)
        {
            if (inStr == null) return null;
            if (inStr.Length < 5) return null;

            return inStr.Mask(0, 4, '*');
        }


        private static string Mask(this string source, int start, int maskLength, char maskCharacter)
        {
            string mask = new string(maskCharacter, maskLength);
            string unMaskStart = source.Substring(0, start);
            string unMaskEnd = source.Substring(start + maskLength);

            return unMaskStart + mask + unMaskEnd;
        }
    }
}
