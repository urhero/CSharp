using System;

class Program
{
    static String toHex(int i)
    {
        string hex = i.ToString("x"); // 대문자 X일 경우 결과 hex값이 대문자로 나온다.
        if (hex.Length % 2 != 0) hex = "0" + hex;
        return hex;
    }
    static void Main()
    {
        string[] l = Console.ReadLine().Split(' ');
        float a0 = 0; //previous a
        float a = Convert.ToInt32(l[0]);
        float b = Convert.ToInt32(l[1]);
        l = Console.ReadLine().Split(' ');
        float av = Convert.ToInt32(l[0]);
        float bv = Convert.ToInt32(l[1]);
        float ut = Convert.ToInt32(l[2]);
        int n;
        for (n = 0; ; n++)
        {
            if (a0 > a || ut > av || n == 51) break;
            a0 = a;
            a = a * (av - ut) / av + b * ut / av;
            b = b * (bv - ut) / bv;
        }
        if (n == 0 || n > 50) Console.WriteLine("gg");
        else Console.WriteLine(toHex(n));
    }
}

