module Fb
  class Connection
    def execute_script(sql)
      stmts = []
      delim = ';'
      while sql =~ /\S/
        stmt, sql = sql.split(delim, 2)
        if stmt =~ /^\s*set\s+term\s+(\S+)/i
          delim = $1
        elsif stmt =~ /\S/
          stmts << stmt
        end
      end
      self.transaction do
        stmts.each do |stmt|
          self.execute(stmt)
        end
      end
    end
  end
end
