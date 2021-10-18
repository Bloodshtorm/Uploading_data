using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Uploading_data
{
    /// <summary>
    /// Главный файл с конфигурацией.
    /// </summary>
    /// <remarks>
    /// Именно по нему определяется, какой формат выгрузки используется. 
    /// Теперь программа загрузки «знает», какие файлы содержит (должна содержать) 
    /// выгрузка и в каком формате хранится представленная в них информация.
    /// </remarks>
    /// <returns>
    /// Класс с данными, они положаться в MKB.txt
    /// </returns>
    public class MKB
    {
        // config MKB.txt
        /*ID String  Ваш уникальный код, сообщается специалистами Резидент Консалтинг.
        Name String  Наименование принципала.
        INN String  ИНН принципала.
        KPP String  КПП принципала.
        D1 Date    Начальная дата периода выгрузки.
        D2 Date    Конечная дата периода выгрузки.
        ATYPE Integer Тип выгрузки:
        0 - полная выгрузка
        1 - краткая выгрузка.
        Prefix  string Префикс, сообщается специалистами Резидент Консалтинг.
        Town    string Наименование населенного пункта, для Челябинска может не указываться*/
        string ID { get; set; }
        string Name { get; set; }
        string INN { get; set; }
        string KPP { get; set; }
        DateTime D1 { get; set; }
        DateTime D2 { get; set; }
        int ATYPE { get; set; }
        string Prefix { get; set; }
        string Town { get; set; }

        public MKB(string ID, string Name, string INN, string KPP, DateTime D1, DateTime D2, int ATYPE, string Prefix, string Town)
        {

        }

        public MKB(string[] arr)
        {
            try
            {
                ID = arr[0];
                Name = arr[1];
                INN = arr[2];
                KPP = arr[3];
                D1 = DateTime.Parse(arr[4]);
                D2 = DateTime.Parse(arr[5]);
                ATYPE = Int32.Parse(arr[6]);
                Prefix = arr[7];
                Town = arr[8];
            }
            catch (Exception ex)
            {
                Console.WriteLine("Ошибка при сопаставлении данных из конфигурации! " + ex.Message);
            }
        }

        public void GetInfo()
        {
            Console.WriteLine(ID);
            Console.WriteLine(Name);
            Console.WriteLine(INN);
            Console.WriteLine(KPP);
            Console.WriteLine(D1.ToString());
            Console.WriteLine(D2.ToString());
            Console.WriteLine(ATYPE.ToString());
            Console.WriteLine(Prefix);
            Console.WriteLine(Town);
        }
        public void CreateFile(string path)
        {
            DirectoryInfo di = Directory.CreateDirectory(path);
            Console.WriteLine("Проверка(создание) дирректории", Directory.GetCreationTime(path));
            string writePath = path + "\\MKB.txt";
            try
            {
                using (StreamWriter sw = new StreamWriter(writePath, false, System.Text.Encoding.Default))
                {
                    sw.WriteLine("ID\tName\tINN\tKPP\tD1\tD2\tATYPE\tPrefix\tTown");
                    sw.WriteLine($"{ID}\t{Name}\t{INN}\t{KPP}\t{D1}\t{D2}\t{ATYPE}\t{Prefix}\t{Town}");
                }
                /*
                using (StreamWriter sw = new StreamWriter(writePath, true, System.Text.Encoding.Default))
                {
                    sw.WriteLine("Дозапись");
                    sw.Write(4.5);
                }
                */
                Console.WriteLine($"Запись {writePath} выполнена");
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }
        }
    }
}
