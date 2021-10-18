using Npgsql;
using System;
using System.Data;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;

namespace Uploading_data
{

    class Program
    {
        //бесполезная строка, просто переменная с нашим коннектом, потом переназначается
        public static string line_con = "Server=192.168.1.51;Database=gkh_chelyabinsk;UserID=postgres;CommandTimeout=36000;Password=1234;Port=5432;";
        public static NpgsqlConnection con = new NpgsqlConnection();
        //куда будут данные ложиться
        public static string[] array = new string[] { "", "РЕГИОНАЛЬНЫЙ ОПЕРАТОР КАПИТАЛЬНОГО РЕМОНТА ОБЩЕГО ИМУЩЕСТВА В МНОГОКВАРТИРНЫХ ДОМАХ ЧЕЛЯБИНСКОЙ ОБЛАСТИ", "7451990794", "745301001", "d1", "d2", "0", "", "MO" };
        public static string path_dirrectory_data = "UploadData";
        /// <summary>
        /// ИД актуального периода. Необходим для разных таблиц.
        /// </summary>
        /// <remarks>
        /// Генерируется при первом подключении
        /// </remarks>
        public static string ActualPeriodId { get; set; }
        public static int IDmo { get; set; }
        public static int Dperiod { get; set; }

        //public static int IDperiod { get; set; }
        static void Main(string[] args)
        {
            Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);
            patch();

        start1:
            Console.WriteLine("Введите ИД периода(только цифры)");
            try
            {
                Dperiod = (int)Convert.ToInt64(Console.ReadLine());
                NpgsqlCommand cmd = new NpgsqlCommand($"SELECT Period_name from regop_period where id={Dperiod}", con);
                Console.WriteLine("Выбран: " + cmd.ExecuteScalar().ToString());

                cmd = new NpgsqlCommand($"SELECT cstart from regop_period where id={Dperiod}", con);
                array[4]=cmd.ExecuteScalar().ToString();

                cmd = new NpgsqlCommand($"SELECT date_trunc('month', cstart+'1month')-'1day'::interval from regop_period where id={Dperiod}", con);
                array[5] = cmd.ExecuteScalar().ToString();
            }
            catch (System.NullReferenceException sys)
            {
                Console.WriteLine(sys.Message);
                goto start1;
            }  
            catch (Exception ex)
            {
                Console.WriteLine("Ошибка: " + ex.Message);
                goto start1;
            }

        start:
            Console.WriteLine("Введите ИД муниципального образования(только цифры)");
            try
            {
                IDmo = (int)Convert.ToInt64(Console.ReadLine());
                NpgsqlCommand cmd = new NpgsqlCommand($"SELECT name from gkh_dict_municipality where id={IDmo.ToString()}", con);
                Console.WriteLine("Выбран: " + cmd.ExecuteScalar().ToString());
                array[8] = cmd.ExecuteScalar().ToString();
            }
            catch (System.NullReferenceException sys)
            {
                Console.WriteLine(sys.Message);
                goto start;
            }
            catch (Exception ex)
            {
                Console.WriteLine("Ошибка: " + ex.Message);
                goto start;
            }

            //выгрузка всех лс
            MKB mkb = new MKB(array);
            mkb.CreateFile(@path_dirrectory_data);

            Console.WriteLine("Выгрузка лицевых счетов, пожалуйста подождите, операция займет некоторое время...");
            DataTable dt = new DataTable();
            NpgsqlDataAdapter sda = new NpgsqlDataAdapter($@"select acc_num ACC_CODE, 
                                        b4fa.street_name as STREET,
                                        b4f.shortname as STR_TYPE,
                                        b4fa.house as HOUSE,
                                        croom_num as FLAT_NUM,
                                        LTRIM(own.name, ' ') as MASTER,
                                        to_char(rpa.open_date, 'DD.MM.YYYY') as DATE_OPEN,
                                        to_char(rpa.close_date, 'DD.MM.YYYY') as DATE_CLOSE,
                                        null AS PHONE,
                                        case 
                                        	when gr.ownership_type = 10 then 'Частная' 
                                        	when gr.ownership_type = 30 then 'Муниципальная' 
                                        	when gr.ownership_type = 40 then 'Государственная'
                                        	when gr.ownership_type = 50 then 'Коммерческая' 
                                        	when gr.ownership_type = 60 then 'Смешанная' 
                                        	when gr.ownership_type = 80 then 'Федеральная'
                                        	when gr.ownership_type = 90 then 'Областная' 
                                        	else 'Не указано' 
                                        end as OWNERSHIP,
                                        case 
                                        	when gr.type = 10 and gr.is_communal = true then 'Коммунальное помещение'
                                        	when gr.type = 10 then 'Жилое помещение'
                                        	when gr.type = 20 then 'Нежилое помещение' 
                                        	else 'Не указано' 
                                        end as HABIT_TYPE,
                                        area_mkd as TOTAL_SQ,
                                        area_living_owned as LIVING_SQ,
                                        null AS LODGER_CNT
                                        from regop_pers_acc rpa
                                        join gkh_room gr on gr.id=rpa.room_id
                                        join regop_pers_acc_owner own on rpa.acc_owner_id = own.id and rpa.state_id = 804
                                        join gkh_reality_object ro on ro.id = gr.ro_id
                                        join b4_fias_address b4fa on b4fa.id= ro.fias_address_id
                                        join b4_fias b4f on b4f.aoguid = b4fa.street_guid and b4f.actstatus=1
                                        where municipality_id={IDmo.ToString()}
                                        order by 2,4,5,6", con);
            sda.Fill(dt);
            Program.CreateFileLs(@path_dirrectory_data, dt);
            Console.WriteLine("Количество лицевых счетов: " + dt.Rows.Count);
            //Освободим лишние ресурсы
            dt = null; 
            sda = null;

            Console.WriteLine("Выгрузка начислений и оплат по лицевым счетам, пожалуйста подождите, операция займет некоторое время...");
            dt = new DataTable();
            sda = new NpgsqlDataAdapter(@$"SELECT
                                        (1) as REC_TYPE,
                                        ac.acc_num as ACC_CODE,
                                        (1) as SRV_ID,
                                        TO_CHAR(psum.object_create_date, 'yyyy.mm.dd')::date DATE,
                                        round(tariff_payment*-1,2) as SUMMA,
                                        'Оплата' as DOC_TEXT
                                        from regop_pers_acc_period_summ psum
                                        join regop_pers_acc ac on ac.id = psum.account_id and ac.state_id = 804
                                        join gkh_room gr on gr.id=ac.room_id
                                        join gkh_reality_object ro on ro.id = gr.ro_id
                                        where psum.period_id = {Dperiod.ToString()} and ro.municipality_id={IDmo.ToString()} and tariff_payment<>0
                                        UNION
                                        select (0) as REC_TYPE,
                                        ac.acc_num as ACC_CODE,
                                        (1) as SRV_ID,
                                        TO_CHAR(psum.object_create_date, 'yyyy-mm-01')::date DATE,
                                        round(charge_tariff,2) as SUMMA,
                                        'Начисление' as DOC_TEXT
                                        from regop_pers_acc_period_summ psum
                                        join regop_pers_acc ac on ac.id = psum.account_id and ac.state_id = 804 
                                        join gkh_room gr on gr.id=ac.room_id
                                        join gkh_reality_object ro on ro.id = gr.ro_id
                                        where psum.period_id = {Dperiod.ToString()} and ro.municipality_id={IDmo.ToString()} and charge_tariff<>0
                                        UNION
                                        SELECT
                                        (2) as REC_TYPE,
                                        ac.acc_num as ACC_CODE,
                                        (1) as SRV_ID,
                                        TO_CHAR(psum.object_create_date, 'yyyy.mm.dd')::date DATE,
                                        round(recalc,2) as SUMMA,
                                        'Перерасчет' as DOC_TEXT
                                        from regop_pers_acc_period_summ psum
                                        join regop_pers_acc ac on ac.id = psum.account_id and ac.state_id = 804 
                                        join gkh_room gr on gr.id=ac.room_id
                                        join gkh_reality_object ro on ro.id = gr.ro_id
                                        where psum.period_id = {Dperiod.ToString()} and ro.municipality_id={IDmo.ToString()}  
                                        and recalc<>0 ORDER BY 2", con);

            sda.Fill(dt);
            Program.CreateFileDoc(@path_dirrectory_data, dt);
            Console.WriteLine("Количество лицевых счетов: " + dt.Rows.Count);
            //Освободим лишние ресурсы
            dt = null;
            sda = null;

            CreateFileUsl(@path_dirrectory_data);

            Console.WriteLine("Выгрузка сальдо по лицевым счетам, пожалуйста подождите, операция займет некоторое время...");
            dt = new DataTable();
            sda = new NpgsqlDataAdapter(@$"select 
                                        acc_num as ACC_CODE,
                                        (1) as SRV_ID,
                                        round(base_tariff_debt,2) as SALDO
                                        from regop_pers_acc_period_summ psum
                                        join regop_pers_acc ac on ac.id = psum.account_id and ac.state_id = 804
                                        join gkh_room gr on gr.id=ac.room_id
                                        join gkh_reality_object ro on ro.id = gr.ro_id
                                        where psum.period_id = {Dperiod.ToString()} and ro.municipality_id={IDmo.ToString()}", con);
            
            sda.Fill(dt);
            Program.CreateFileSaldo(@path_dirrectory_data, dt);
            Console.WriteLine("Количество лицевых счетов: " + dt.Rows.Count);

            con.Close();
            GC.Collect();
            Console.WriteLine("Успешно! Нажмите любую клавишу для закрытия консоли");
            Console.ReadLine();


        }

        public static void CreateFileLs(string path, DataTable dt)
        {
            DirectoryInfo di = Directory.CreateDirectory(path);
            Console.WriteLine("Проверка(создание/пересоздание) дирректории *\\LS.txt", Directory.GetCreationTime(path));

            string writePath = path + "\\LS.txt";
            try
            {
                using (StreamWriter sw = new StreamWriter(writePath, false, Encoding.GetEncoding(1251)))
                {
                    sw.WriteLine("ACC_CODE\tSTREET\tSTR_TYPE\tHOUSE\tFLAT_NUM\tMASTER\tDATE_OPEN\tDATE_CLOSE\tPHONE\tOWNERSHIP\tHABIT_TYPE\tTOTAL_SQ\tLIVING_SQ\tLODGER_CNT");
                    foreach (DataRow row in dt.Rows)
                    {
                        sw.WriteLine($"{row[0]}\t{row[1]}\t{row[2]}\t{row[3]}\t{row[4]}\t{row[5]}\t{row[6]}\t{row[7]}\t{row[8]}\t{row[9]}\t{row[10]}\t{row[11]}\t{row[12]}\t{row[13]}");
                    }
                    //sw.WriteLine($"{REC_TYPE}\t{ACC_CODE}\t{SRV_ID}\t{DATE}\t{SUMMA}\t{DOC_TEXT}");
                    sw.Dispose();
                }
                Console.WriteLine($"Запись {writePath} выполнена");

            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }
        }

        public static void CreateFileDoc(string path, DataTable dt)
        {
            DirectoryInfo di = Directory.CreateDirectory(path);
            Console.WriteLine("Проверка(создание/пересоздание) дирректории *\\DOC.txt", Directory.GetCreationTime(path));

            string writePath = path + "\\DOC.txt";
            try
            {
                using (StreamWriter sw = new StreamWriter(writePath, false, Encoding.GetEncoding(1251)))
                {
                    sw.WriteLine("REC_TYPE\tACC_CODE\tSRV_ID\tDATE\tSUMMA\tDOC_TEXT");
                    foreach (DataRow row in dt.Rows)
                    {
                        sw.WriteLine($"{row[0]}\t{row[1]}\t{row[2]}\t{row[3]}\t{row[4]}\t{row[5]}");
                    }
                    //sw.WriteLine($"{REC_TYPE}\t{ACC_CODE}\t{SRV_ID}\t{DATE}\t{SUMMA}\t{DOC_TEXT}");
                }
                Console.WriteLine($"Запись {writePath} выполнена");
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }
        }

        //услуга одна код 1 наиманование "Взносы за капитальный ремонт"
        public static void CreateFileUsl(string path)
        {
            DirectoryInfo di = Directory.CreateDirectory(path);
            Console.WriteLine("Проверка(создание/пересоздание) дирректории *Usl.txt", Directory.GetCreationTime(path));

            string writePath = path + "\\Usl.txt";
            try
            {
                using (StreamWriter sw = new StreamWriter(writePath, false, Encoding.GetEncoding(1251)))
                {
                    sw.WriteLine("SRV_ID\tSRV_NAME");
                    sw.WriteLine("1\tВзносы за капитальный ремонт");
                    Console.WriteLine($"Запись услуги в {writePath} выполнена");
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }
        }

        public static void CreateFileSaldo(string path, DataTable dt)
        {
            DirectoryInfo di = Directory.CreateDirectory(path);
            Console.WriteLine("Проверка(создание/пересоздание) дирректории *Saldo.txt", Directory.GetCreationTime(path));

            string writePath = path + "\\Saldo.txt";
            try
            {
                using (StreamWriter sw = new StreamWriter(writePath, false, Encoding.GetEncoding(1251)))
                {
                    sw.WriteLine("ACC_CODE\tSRV_ID\tSALDO");
                    foreach (DataRow row in dt.Rows)
                    {
                        sw.WriteLine($"{row[0]}\t{row[1]}\t{row[2]}");
                    }
                }
                Console.WriteLine($"Запись {writePath} выполнена");
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }
        }
        static void patch()
        {

        start:
            //con = new NpgsqlConnection(line_con);
            Console.WriteLine("Строка подключения по умолчанию:\n" + line_con);
            Console.WriteLine("Введите новую строку, или нажмите Enter если хотите оставить без изменений");
            string new_line_con = Console.ReadLine();
            if (new_line_con != "") //в случае если строку подключения надо поменять
            {
                line_con = new_line_con;
            }
            try
            {
                con = new NpgsqlConnection(line_con);
                con.ConnectionString = line_con;
                Console.WriteLine("Попытка подключения...");
                con.Open();
                NpgsqlCommand cmd = new NpgsqlCommand("select max(id) from regop_period", con);
                ActualPeriodId = cmd.ExecuteScalar().ToString();
                Console.WriteLine("ID актуального периода: " + ActualPeriodId);

            }
            catch
            {
                Console.WriteLine("Подключение отсутствует!", "Ошибка");
                goto start;
            }
        }

        static string[] ArrayMKD()
        {
            string[] arraymkd = new string[] { };
            try
            {
                string path = @"READ_ME_StartConfig.cfg";

                using (StreamReader sr = new StreamReader(path, System.Text.Encoding.Default))
                {
                    string line;
                    //Читаем не пустые строки, и строки без решетки вначале(#-комментарии)
                    while (((line = sr.ReadLine()) != null))
                    {
                        if (line.Remove(1, line.Length - 1) != "#")
                        {
                            //Console.WriteLine(line);
                            if (line.Remove(1, line.Length - 1) == "*")
                            {
                                line_con = line.Replace("*", "");
                                Console.WriteLine("Из файла конфигурации, строка подключения:\n" + line.Replace("*", ""));
                                //line_con=
                            }

                            if (line.Remove(1, line.Length - 1) == "+")
                            {
                                Console.WriteLine("Из файла конфигурации, свойства выгрузки:\n" + line.Replace("+", ""));
                                arraymkd = line.Replace("+", "").Split("\t");
                            }
                        }
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }
            return (arraymkd);
        }

    

       
    }
}
