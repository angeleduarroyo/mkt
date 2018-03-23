# coding=utf-8
import sys
from pyspark.sql.functions import udf
from pyspark.sql.functions import col as c
from pyspark.sql.functions import desc, when, date_format, lag, count, avg, stddev, lit, year, month, max, min, sum
from pyspark.sql.functions import row_number, percent_rank
from pyspark.sql.window import Window
from dateutil.relativedelta import relativedelta
from datetime import date, timedelta, datetime
from pyspark.sql.types import FloatType, DateType
import conf.tla1872_schema as fields
from utils.analytic_utils import AnalyticUtils


class Tla1872Recurrency:

    def filter_active_users(self, end_date, months_back=3):
        """
        This function filters the users that haven't had activity since n back.
        This ensures that we only consider active users to look for the recurrent transactions from tla1872.
        It gets the current date and how many months back you want to consider teh default is three months.

        :param end_date:it takes the current date
        :param months_back: it takes 'n' months back of the operation date
        :return: Dataframe filtered by active users
        """


        activation_date = datetime.strptime(end_date, "%Y-%m-%d").date()
        number_months_back = activation_date - relativedelta(months=months_back)
        users_active = self.tla1872.where((c(fields.operation_date) >= number_months_back)). \
            select(fields.customer_id).distinct()
        print("Active customers, " + str(users_active.count()) + " registers.")
        self.worktable = self.tla1872.join(users_active, on=fields.customer_id)
        print("Filtered transactions, " + str(self.worktable.count()) + " registers.")

    def filter_worktable(self):

        """
        This function filters the fields from tla1872 leaving only the ones that are relevant to find
        recurrent transactions.
        :return: worktable dataframe filtered by  some relevant fields
        """


        self.worktable = self.worktable.select(fields.customer_id, fields.operation_id,
                                               fields.return_type, fields.debit_type, fields.line_of_business_id,
                                               fields.commerce_affiliation_id, fields.message_type,
                                               fields.operation_date, fields.operation_time_date,
                                               fields.transaction_amount, fields.authorization_reference_number,
                                               fields.operation1_id, fields.partitioning_type,
                                               fields.operation_card_owner_type, fields.lynx_validation_type,
                                               fields.original_transaction_amount)

    def delete_reverses(self):
        """
        This function deletes transactions that have been reversed along with their original version. It uses
        a flag called "flag_reverse" to identify the reversed transactions using the message type.
        :return: worktable dataframe without reversed transactions
        """

        reverses = Window.partitionBy([fields.customer_id, fields.operation_id, fields.return_type,
                                       fields.operation_date, fields.operation_time_date, fields.transaction_amount,
                                       fields.authorization_reference_number, fields.operation1_id]). \
            rowsBetween(-sys.maxsize, sys.maxsize)
        self.worktable = self.worktable.select(fields.customer_id,  fields.operation_id,
                                               fields.return_type, fields.debit_type, fields.line_of_business_id,
                                               fields.commerce_affiliation_id, fields.message_type,
                                               fields.operation_date, fields.operation_time_date,
                                               fields.transaction_amount, fields.authorization_reference_number,
                                               fields.operation1_id, fields.partitioning_type,
                                               fields.operation_card_owner_type, fields.lynx_validation_type,
                                               fields.original_transaction_amount,
                                               when(max(c(fields.message_type)).over(reverses) >= '1420', 1)
                                               .otherwise(0).alias('flag_reverse')
                                               )
        self.worktable = self.worktable.where(c('flag_reverse') == 0)
        print("Registers after reverses: " + str(self.worktable.count()))

    def divide_recurrents_no_recurrents(self):
        """
        This function identifies the functions that are marked as recurrents by defaul in debit_type of
        tla1872 and separates them from the ones that are not labeled that way
        :return: Two Dataframes separates by recurrents and no recurrents
        """

        self.recurrents_worktable = self.worktable.where(c(fields.debit_type) == 'CR')
        self.recurrents_worktable = self.recurrents_worktable.groupBy(
            fields.customer_id, fields.commerce_affiliation_id, fields.operation_date).agg(
            sum(fields.transaction_amount).alias("transaction_amount"))
        self.no_recurrents_worktable = self.worktable.where(c(fields.debit_type) != 'CR')
        print("Recurrents, " + str(self.recurrents_worktable.count()) + " registers.")
        print("No Recurrents, " + str(self.no_recurrents_worktable.count()) + " registers.")

    @staticmethod
    def generate_variables(dataframe, partition_array, order_array):
        """
        Generates new variables to identify recurrent payments. It calculates the difference of days
        (difference_of_days) between transactions of the same amount, made in the same commerce;
         the number of such transactions (nu_payment); the amount payed (impoper_f);
        and the order of the transactions of the same kind made more than once.
        It returns a data frame with this information
        :param dataframe:
        :param partition_array:['customer_id', 'commerce_name']
        :param order_array: ['operation_date']
        :return: Dataframe identifying  recurrent payments according to the order of the trasnsactions.
        """

        utils = AnalyticUtils
        filtered_window = Window.partitionBy(partition_array).orderBy(order_array)
        filtered_descending = Window.partitionBy(fields.customer_id, fields.commerce_affiliation_id) \
            .orderBy(desc(fields.operation_date))
        filtered_by_range = Window.partitionBy(partition_array).orderBy(order_array) \
            .rowsBetween(-sys.maxsize, sys.maxsize)
        return dataframe.select(
            fields.customer_id, fields.commerce_affiliation_id, fields.transaction_amount,  fields.operation_date,
            (utils.subtract_days(date_format(c(fields.operation_date), 'YYYY-MM-dd'),
                                date_format(lag(fields.operation_date).over(filtered_window), 'YYYY-MM-dd'))).alias(
                'difference_of_days'), lag(fields.transaction_amount).over(filtered_window).alias('impoper_f'),
            utils.retrieve_day(date_format(c(fields.operation_date), 'YYYY-MM-dd')).alias('day_month'),
            count('*').over(filtered_by_range).alias('nu_payments'),
            row_number().over(filtered_window).alias('order'), row_number().over(filtered_descending).alias('order2'))

    def get_recurrents_amount(self):
        """
         Gets the recurrents (repited more than twice) in amount from the ones that come marked by
        default in the table as recurrents in divide_recurrents_no_recurrents (self.recurrents_worktable).
        It generates two data frame with recurrents and not recurrents transactions according their
        amount or "importe" classified as amount_recurrents_worktable and amount_no_recurrents_worktable.
        To be "amount_recurrents" the amount payed must be always the same.
        :return: Two Dataframes separated by recurrent and no recurrent amounts by customer  commerce and transaction
        """



        partitition_array = ['customer_id', 'commerce_affiliation_id', 'transaction_amount']
        order_array = ['operation_date']
        payments_worktable = self.generate_variables(self.recurrents_worktable, partitition_array, order_array)
        # Filtering payments recurrents on import
        self.amount_recurrents_worktable = payments_worktable.where(c('nu_payments') > 2)
        # Filtering payments no recurrents on import, can be recurrents only commerce
        self.amount_no_recurrents_worktable = payments_worktable.where(c('nu_payments') <= 2)
        print("Evaluated registers - Amount, " + str(payments_worktable.count()))
        print("Amount recurrents, " + str(self.amount_recurrents_worktable.count()) + " registers.")
        print("Amount no recurrents, " + str(self.amount_no_recurrents_worktable.count()) + " registers.")

    def get_recurrents_commerce(self):
        """
        Gets the recurrents in commerce. This are those transactions that, even if they are not marked as
        recurrents by default (no_recurrents_worktable), or failed to be classified as recurrents amount
        (amount_no_recurrents_worktable) are recurrent. They are classified as recurrents given that they
        are being done in the same commerce, paying a similar or equal amount each time and with more than
        2 repetitions. this methods modifies:
        -amount_no_recurrents_worktable: making a selection of variables
        -no_recurrents_worktable: making a selection of variables
        -commerce_recurrents_worktable: new recurrents found
        -no_recurrents_worktable: actualization considering the new filter
         :return: Two Dataframes separated by recurrent and no recurrent amounts by customer and commerce

        """
        partition_array = ['customer_id', 'commerce_affiliation_id']
        order_array = ['operation_date']
        self.amount_no_recurrents_worktable = self.amount_no_recurrents_worktable.select(
            fields.customer_id, fields.commerce_affiliation_id,  fields.transaction_amount,
            fields.operation_date)
        self.no_recurrents_worktable = self.no_recurrents_worktable.select(
            fields.customer_id, fields.commerce_affiliation_id,  fields.transaction_amount,
            fields.operation_date)
        # Union between 1872 not recurrents rows AND last output: No recurrents amount
        payments_number_worktable = self.generate_variables(
            (self.amount_no_recurrents_worktable.union(self.no_recurrents_worktable)), partition_array, order_array)
        # Filtering payments recurrents on commerce
        self.commerce_recurrents_worktable = payments_number_worktable.where(c('nu_payments') > 2)
        # Filtering payments NO recurrents (unique payments)
        self.no_recurrents_worktable = payments_number_worktable.where(c('nu_payments') <= 2)  # First no recurrents
        print("Evaluated registers - Commerce, " + str(payments_number_worktable.count()))
        print("Recurrents in commerce, " + str(self.commerce_recurrents_worktable.count()) + " registers.")
        print("NO Recurrents, " + str(self.no_recurrents_worktable.count()) + " registers.")

    @staticmethod
    def aggregate_customer_level(dataframe, group_array):

        """
        Aggregating customer level variables to get mean and SD, and SRE of days of difference (diff_days), day
        of month and amount.
        Mins and maxs are also calculated. This information will be necessary to make the last filters for
        recurrents and none recurrents transactions.
        :param dataframe: It takes the amount_recurrents_worktable dataframe which has the recurrents
        :param group_array: It takes the following fields ['customer_id', 'commerce_affiliation_id', 'transaction_amount'] to group dataframe
        :return:Gets Dataframe statistics  varibles group by customer and commerce field
        """

        utils = AnalyticUtils
        return dataframe.groupBy(group_array).agg(
            # Means
            avg(c(fields.transaction_amount).cast('Float')).cast('Float').alias("MEAN_transaction_amount"),
            avg(c('day_month').cast('Float')).cast('Float').alias("MEAN_DAY_MONTH"),
            avg(when(c('order') != 1, c('difference_of_days').cast('Float'))).cast('Float').alias("MEAN_DIFF_DAYS"),
            max(c("nu_payments").cast("Float")).alias('num_pagos'),
            # Standard Deviations
            stddev(c(fields.transaction_amount).cast('Float')).cast('Float').alias("SD_transaction_amount"),
            stddev(c('day_month').cast('Float')).cast('Float').alias("SD_dia_mes"),
            stddev(when(c('order') != 1, c('difference_of_days').cast('Float'))).cast('Float').alias("SD_days_diff"),
            # max and mins
            max(c(fields.transaction_amount).cast("Float")).alias("MAX_transaction_amount"),
            min(c(fields.transaction_amount).cast("Float")).alias("MIN_transaction_amount"),
            max(c('day_month').cast("Integer")).alias("MAX_day_month"),
            min(c('day_month').cast("Integer")).alias("MIN_day_month"),
            max(c('difference_of_days').cast("Integer")).alias("MAX_days_diff"),
            min(when(c('order') != 1, c('difference_of_days').cast("Integer"))).alias("MIN_days_diff"),
            max(c(fields.operation_date)).alias("MAX_FECHA"),
            # ranges
            ((max(c(fields.transaction_amount).cast("Float"))) - (
                min(c(fields.transaction_amount).cast("Float")))).cast("Float").alias("RANGE_transaction_amount"),
            (max(c('day_month').cast("Integer")) - min(c('day_month').cast("Integer"))).alias("RANGE_days_month"),
            (max(c('difference_of_days').cast("Integer")) - min(
                when(c('order') != 1, c('difference_of_days').cast("Integer")))).alias("RANGE_days_diff"),
            # SRE
            (stddev(c(fields.transaction_amount).cast('Float')).cast('Float') /
             (utils.squared_column(max(c("nu_payments").cast("Float")).cast("Float"))*avg(
                 c(fields.transaction_amount).cast('Float')).cast('Float'))).alias("SRE_transaction_amount"),
            (stddev(c('day_month').cast('Float')).cast('Float') / (
                utils.squared_column(max(c("nu_payments").cast("Float")).cast("Float"))*avg(
                    c('day_month').cast('Float')).cast('Float'))).alias("SRE_dia_mes"),
            (stddev(when(c('order') != 1, c('difference_of_days').cast('Float'))).cast('Float') /
             (utils.squared_column((max(c("nu_payments").cast("Float"))-1).cast("Float"))
              * avg(when(c('order') != 1, c('difference_of_days').cast('Float'))).cast('Float'))).alias("SRE_days_diff")
        )

    @staticmethod
    def get_last_amount_recurrency(recurrents_filter_worktable, group_array):
        """
        Getting statistics (average , standard deviation and coefficient of variation )on transaction_amount field and it is group by customer and commerce
        :param recurrents_filter_worktable:Recurrents Dataframe
        :param group_array: Array  groups by  the following fields['customer_id', 'commerce_affiliation_id']
        :return: Recurrents Dataframe contains transaction_amount statistics group by group_array parameters
        """
        return recurrents_filter_worktable.groupBy(group_array).agg(
            # means
            avg(when(c('order2') <= 4, c(fields.transaction_amount).cast('Float'))).cast('Float').alias(
                "MEAN_transaction_amount"),
            # SD
            stddev(when(c('order2') <= 4, c(fields.transaction_amount).cast('Float'))).cast('Float').alias(
                "SD_transaction_amount"),
            # CV
            ((stddev(when(c('order2') <= 4, c(fields.transaction_amount).cast('Float'))).cast('Float'))/(avg(
                when(c('order2') <= 4, c(fields.transaction_amount).cast('Float'))
            ).cast('Float'))).cast('Float').alias("CV_transaction_amount"))

    def get_customer_level_variables(self):

        """
        Build up the customer level variables using aggregate_customer_level static method
        :return:recurrents_commerce_vars_worktable dataframe which has agregated variables for recurrents in commerce
        """
        group_amount_array = ['customer_id', 'commerce_affiliation_id', 'transaction_amount']
        group_commerce_array = ['customer_id', 'commerce_affiliation_id']
        selection = ['customer_id', 'commerce_affiliation_id', 'transaction_amount', 'operation_date']
        # Amount Recurrents
        self.amount_vars_recurrents_worktable = self.aggregate_customer_level(self.amount_recurrents_worktable,
                                                                              group_amount_array)
        # Getting Variability Coeficient for filter between similar Amounts and no similar Amounts
        recurrents_for_filter_worktable = self.get_last_amount_recurrency(self.commerce_recurrents_worktable,
                                                                          group_commerce_array)
        # Getting rows with similar amounts
        recurrents_commerce_filtered_worktable = self.commerce_recurrents_worktable. \
            join(recurrents_for_filter_worktable, on=group_commerce_array). \
            withColumn("CV_filter", when((recurrents_for_filter_worktable.CV_transaction_amount < 0.5),
                                         lit(1)).otherwise(0))
        # Separate wtRecurrentsCommerce [ Recurrents and NO Recurrents]
        self.commerce_recurrents_worktable = recurrents_commerce_filtered_worktable.select('*') \
            .filter(c("CV_filter") == 1) \
            .drop('MEAN_transaction_amount') \
            .drop('SD_transaction_amount') \
            .drop('CV_transaction_amount')
        # SEGUNDAS NO RECURRENTES
        self.no_recurrents_worktable = (self.no_recurrents_worktable.select(selection)) \
            .union(recurrents_commerce_filtered_worktable.select(selection).filter(c("CV_filter") == 0))
        # Agregated Variables for Recurrents in Commerce
        self.recurrents_commerce_vars_worktable = self.aggregate_customer_level(self.commerce_recurrents_worktable,
                                                                                group_commerce_array)

    @staticmethod
    def get_information_transaction_level(aggregate_dataframe, level_dataframe, group_array):
        """
         Making join between  dataframe according to customer_id and commerce fields.
        :param aggregate_dataframe:Dataframe
        :param level_dataframe:Dataframe
        :param group_array:  Array takes the following fields ['customer_id', 'commerce_affiliation_id', 'transaction_amount']
                            or  ['customer_id', 'commerce_affiliation_id']
        :return:Dataframe from joining level_dataframe and aggrefate_dataframe
        """
        return level_dataframe.join(aggregate_dataframe, on=group_array)

    @staticmethod
    def get_date_difference(this_date, delta_sd, delta_mean):
        """
        Gets the last payment and adds to it the mean and 2SD of dias diff (delta)
        :param this_date: takes maximum  transaction date
        :param delta_sd:  Float type  param that  takes Standard Deviations of difference days transactions
        :param delta_mean: Float type  that takes average difference days transactions
        :return: Dataframe  Calculates a period of time from the maximum transaction date (the mean plus 2SD)
        """

        start = datetime.strptime(str(this_date), '%Y-%m-%d')
        delta = (2*int(delta_sd))+int(delta_mean)
        end_time = start + timedelta(days=delta)
        return end_time

    def separate_recurrents_no_recents(self, recurrents, level_dataframe, group_array):
        """
        Consider as non recurrents all that have been recurrents but haven't has a payment in a time longer
        than the mean plus 2SD
        :param recurrents:Dataframe
        :param level_dataframe:Dataframe
        :param group_array: Array that groups by  the following fields
               ['customer_id', 'commerce_affiliation_id', 'transaction_amount'] for amount recurrents and
               ['customer_id', 'commerce_affiliation_id'] for commerce recurrents dataframes
        :return Two Dataframes Recurrents and Recurrents no recents)

        """

        get_day_difference_udf = udf(lambda date_start, delta_sd, delta_mean:
                                     Tla1872Recurrency.get_date_difference(
                                         date_start, delta_sd, delta_mean), DateType())
        recurrents_vars = recurrents.where(self.final_date <= (
            get_day_difference_udf(c("MAX_FECHA"), c("SD_days_diff"), c("MEAN_DIFF_DAYS"))))
        no_recurrent_vars = recurrents.where(self.final_date > (
            get_day_difference_udf(c("MAX_FECHA"), c("SD_days_diff"), c("MEAN_DIFF_DAYS"))))
        return recurrents_vars, self.get_information_transaction_level(no_recurrent_vars, level_dataframe, group_array)

    def discard_recurrents_no_recents(self):

        """
        Uses separate recurrents no recents to eliminate all recurrents that haven't had a
        transaction recently
        :return: Two Dataframes Recurrents and Recurrents no recents)
        """


        group_amount_array = ['customer_id', 'commerce_affiliation_id', 'transaction_amount']
        group_commerce_array = ['customer_id', 'commerce_affiliation_id']
        selection = ['customer_id', 'commerce_affiliation_id', 'transaction_amount', 'operation_date']
        self.amount_vars_recurrents_worktable, no_recurrents_amount_vars = \
            self.separate_recurrents_no_recents(self.amount_vars_recurrents_worktable,
                                                self.amount_recurrents_worktable, group_amount_array)
        self.recurrents_commerce_vars_worktable, no_recurrents_commerce_vars = \
            self.separate_recurrents_no_recents(self.recurrents_commerce_vars_worktable,
                                                self.commerce_recurrents_worktable, group_commerce_array)
        self.no_recurrents_worktable = self.no_recurrents_worktable.union(no_recurrents_amount_vars.select(selection)) \
            .union(no_recurrents_commerce_vars.select(selection))

    def divide_recurrents(self, recurrents, level_dataframe, group_array):

        """
        Classifies the recurrent transactions found in hard filteed recurrents, soft filtered recurrents and
        not recurrent based on the range of days of difference, the mean of days fo difference, and the SRE
        of the days of difference.
        :param recurrents: Dataframe
        :param level_dataframe: Dataframe
        :param group_array: Array that groups by  the following fields
               ['customer_id', 'commerce_affiliation_id', 'transaction_amount'] for amount recurrents and
               ['customer_id', 'commerce_affiliation_id'] for commerce recurrents dataframes
        :return: three Dataframes Hard filter soft filter , no recurrents
        """

        selection = ['customer_id', 'commerce_affiliation_id', 'transaction_amount', 'operation_date']
        hard_filter = recurrents.filter((c("RANGE_days_diff") < c("MEAN_DIFF_DAYS")) & (c("SRE_days_diff") < 0.045))
        soft_filter = recurrents.filter((c("RANGE_days_diff") < c("MEAN_DIFF_DAYS")) & (c("SRE_days_diff") > 0.045))
        no_recurrents = self.get_information_transaction_level(
            (recurrents.filter(c("RANGE_days_diff") > c("MEAN_DIFF_DAYS"))),
            level_dataframe, group_array).select(selection)
        return hard_filter, soft_filter, no_recurrents

    def categorize_recurrents(self):

        """
        This function uses divide_recurrents method to classify amount_vars_recurrents_worktable in hard
        and soft
        and ??? amount_recurrents_worktable
        :return:three Dataframes Hard filter soft filter , no recurrents
        """

        group_amount_array = ['customer_id', 'commerce_affiliation_id', 'transaction_amount']
        group_commerce_array = ['customer_id', 'commerce_affiliation_id']
        selection = ['customer_id', 'commerce_affiliation_id', 'transaction_amount', 'operation_date']
        self.recurrents_amount_vars_hard, self.recurrents_amount_vars_soft, no_recurrents_amount_vars = \
            self.divide_recurrents(self.amount_vars_recurrents_worktable, self.amount_recurrents_worktable,
                                   group_amount_array)
        self.recurrents_commerce_vars_hard, self.recurrents_commerce_vars_soft, no_recurrents_commerce_vars = \
            self.divide_recurrents(self.recurrents_commerce_vars_worktable,
                                   self.commerce_recurrents_worktable, group_commerce_array)
        self.no_recurrents_worktable = self.no_recurrents_worktable.union(no_recurrents_amount_vars.select(selection)) \
            .union(no_recurrents_commerce_vars.select(selection))

    @staticmethod
    def get_percentile_no_recurrence(no_recurrents_work):

        """
        Get the percentile to assign the amount that corresponds to non recurrent payments
        :param no_recurrents_work: Dataframe
        :return: Dataframe
        """

        filtered_window = Window.partitionBy(fields.customer_id).orderBy("sum_importe")
        ranged_window = Window.partitionBy(fields.customer_id).rowsBetween(-sys.maxsize, sys.maxsize)
        no_recurrents = no_recurrents_work.select('*', percent_rank().over(filtered_window).alias("percentile"),
                                                  count('*').over(ranged_window).alias('num_rows'))

        return no_recurrents.select('*', when(c("percentile") >= (1-(1/c("num_rows"))),
                                              c("sum_importe")).otherwise(None).alias("percentile_im"))

    @staticmethod
    def get_average_days(no_recurrents_work, number):

        """
         Get Min import of the closest percentile larger than the wished percentile by month
        :param no_recurrents_work:Dataframe
        :param number: Integer days number
        :return:Dataframe
        """

        aggregations_date = [(min("percentile_im")/number).alias("month_im")]
        return no_recurrents_work.groupBy(fields.customer_id).agg(*aggregations_date)

    def no_recurrency_analysis(self, days_number):

        """
        This function returns the amount per day for transactions that don't match with recurrent cases
        :param days_number: Integer
        :return: DataFrame
        """


        self.no_recurrents_worktable = self.no_recurrents_worktable.groupBy(
            fields.customer_id, year(fields.operation_date).alias('year'), month(fields.operation_date).alias('month')) \
            .agg(sum(fields.transaction_amount).alias("sum_importe"))
        percentiles = self.get_percentile_no_recurrence(self.no_recurrents_worktable)
        self.no_recurrents_grouped = self.get_average_days(percentiles, days_number)

    def unify_recurrents(self):
        """
        This function appends  all  recurrent dataframes from union between four dataframes recurrenst amount
        (hard and soft) and recurrents commerce (hard and soft)
        :return:Dataframe
        """

        selection = [c(fields.customer_id).alias('customer_id'),
                     c('MEAN_transaction_amount').alias("imp_oper"),
                     c('MAX_FECHA').alias("max_fec_trans"),
                     c('MEAN_DIFF_DAYS').alias("delta_avg"),
                     c('SD_days_diff').alias("delta_std")]
        self.recurrents = self.recurrents_amount_vars_hard.select(selection).union(
            self.recurrents_amount_vars_soft.select(selection)).union(
            self.recurrents_commerce_vars_soft.select(selection)).union(
            self.recurrents_commerce_vars_hard.select(selection))

    @staticmethod
    def is_on_range(date_begin_string, amount, max_date_string, delta, delta_std, current_date_string):
        """
        Calculates recurrent payment comparing:  next payment formula 2 standar desviations and begin and current date
        :param date_begin_string:Date
        :param amount:Float
        :param max_date_string: Date as String
        :param delta:Float
        :param delta_std:Float
        :param current_date_string: Date as String
        :return: Float Type amount
        """


        date_current = datetime.strptime(current_date_string, "%Y-%m-%d").date() \
            if not(isinstance(current_date_string, date)) else current_date_string
        date_begin = datetime.strptime(date_begin_string, "%Y-%m-%d").date() \
            if not(isinstance(date_begin_string, date)) else date_begin_string
        max_date = datetime.strptime(max_date_string, "%Y-%m-%d").date() \
            if not(isinstance(max_date_string, date)) else max_date_string
        days_until_payment = (date_current - max_date).days % delta
        # Eventually it will result zero
        days_until_payment = delta if days_until_payment == 0 else days_until_payment
        next_payment = int(delta - days_until_payment)  # We only care about the next payment
        # Get the approximated next payment
        if next_payment == int(2*delta_std):
            return amount
        elif next_payment < 2*delta_std and date_begin == date_current:
            # The recurrent payment has not been done, and it is less than 2 SDs before average
            return amount
        elif next_payment >= (delta - 2*delta_std) and date_begin == date_current:
            # The recurrent payment has not been done, and it is more than 2 SDs after average
            return amount
        else:
            return 0.0

    def predict_recurrency(self, start_date, end_date):
        """
        Creates recurrent Dataframe adding  sumarization of dates  field
        :param start_date: Date
        :param end_date: Date
        :return: Dataframe
        """

        # Get the amount only if is applicable in the date
        is_applicable = udf(Tla1872Recurrency.is_on_range, FloatType())
        activation_date = start_date

        # Getting the expressions in order to get the application and summarization of the date
        aggregated_dates = []
        aggregated_sums = []
        while activation_date < end_date:
            aggregated_dates.append(is_applicable(
                lit("{}".format(start_date)),
                c("imp_oper"),
                c("max_fec_trans"),
                c("delta_avg"),
                c("delta_std"),
                lit("{}".format(activation_date))
            ).alias(str(activation_date))
                                    )
            aggregated_sums.append(sum("{}".format(activation_date)).alias(str(activation_date)))
            activation_date = activation_date + timedelta(days=1)
        recurrent_dates = self.recurrents.select("*", *aggregated_dates)
        recurrent_amounts = recurrent_dates.groupBy("customer_id").agg(*aggregated_sums)
        return recurrent_amounts

    def predict_no_recurrency(self, start_date, end_date):

        """
        This function returns the calendar for transactions that don't match with recurrent cases
        :param start_date: Date
        :param end_date: Date
        :return: Dataframe
        """

        activation_date = start_date
        aggregated_dates = []
        while activation_date < end_date:
            aggregated_dates.append(c("month_im").alias("{}".format(activation_date)))
            activation_date = activation_date + timedelta(days=1)
        return self.no_recurrents_grouped.select("customer_id", *aggregated_dates)

    def __init__(self):
        self.tla1872 = None
        self.worktable = None
        self.recurrents_worktable = None
        self.no_recurrents_worktable = None
        self.amount_recurrents_worktable = None
        self.amount_no_recurrents_worktable = None
        self.commerce_recurrents_worktable = None
        self.no_recurrents_worktable = None
        self.amount_vars_recurrents_worktable = None
        self.recurrents_commerce_vars_worktable = None
        self.amount_no_recurrents_worktable = None
        self.recurrents_amount_vars_hard = None
        self.recurrents_amount_vars_soft = None
        self.recurrents_commerce_vars_hard = None
        self.recurrents_commerce_vars_soft = None
        self.recurrents = None
        self.no_recurrents_grouped = None
        self.abt = None
        self.final_date = None
        self.auxLuis = None

    def fit(self, tla1872_data, origin_date=None):

        """
         Calls all necesary 1872 class  functions in order create  the final recurrents Dataframe
         fitted for the liquid analisys
        :param tla1872_data: Dataframe fromo TLA1872
        :param origin_date: Date
        :return:Dataframe
        """
        # Get last transaction
        if origin_date is None:
            self.final_date = tla1872_data.select(max(fields.operation_date).alias("max")).collect()[0]["max"]
        else:
            self.final_date = "{}".format(origin_date)
        self.tla1872 = tla1872_data
        self.filter_active_users(str(self.final_date))
        self.delete_reverses()
        self.divide_recurrents_no_recurrents()
        self.get_recurrents_amount()
        self.get_recurrents_commerce()
        self.get_customer_level_variables()
        self.discard_recurrents_no_recents()
        self.categorize_recurrents()
        self.unify_recurrents()
        number_days = 30
        self.no_recurrency_analysis(number_days)

    def predict(self, begin_date=date(2017, 4, 30), end_date=date(2017, 5, 30)):

        """
         Predict the amount between date_begin and date_end

        :param begin_date: Date
        :param end_date: Date
        :return:Recurrent and no recurrents Datafarmes
        """



        recurrents_calendar = self.predict_recurrency(begin_date, end_date)
        no_recurrents_calendar = self.predict_no_recurrency(begin_date, end_date)
        return recurrents_calendar, no_recurrents_calendar
