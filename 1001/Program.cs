using System;
class P
{
    static void Main()
    {
        string[] l = Console.ReadLine().Split(' ');
        int n = Convert.ToInt32(l[0]);
        for (int i = n; i > 0; i--)
            Console.WriteLine(i);
    }
}