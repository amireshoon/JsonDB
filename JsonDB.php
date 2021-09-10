<?php

class JsonDB {
    
    protected $db_file_path;
    private $tables = [];
    public function __construct($path = __DIR__ ) {
        $this->db_file_path = $path;
    }

    public function insert($table, $data) {
        $this->get_table($table);
        
        $validations = $this->column_exists($table, array_keys($data));

        foreach ($validations as $key => $value)
            if (false === $value)
                throw new Exception("Column $key not found", 1);

        $schema = $this->tables[$table]['schema'];

        foreach ($data as $column => $value) {
            if (!$schema[$column]['nullable'] && is_null($value)) {
                if (!array_key_exists('default', $schema[$column])) {
                    throw new Exception("No value provided for column $column", 1);
                }else {
                    $data[$column] = $schema[$column]['default'];
                }
            }else {
                if (is_null($value) && array_key_exists('default', $schema[$column])) {
                    $data[$column] = $schema[$column]['default'];
                }
            }
        }
        
        foreach ($this->tables[$table]['schema'] as $column => $value) {
            if (!array_key_exists($column, $data)) {
                if ($value['nullable']) {
                    if (array_key_exists('default', $this->tables[$table]['schema'][$column])) {
                        $data[$column] = $value['default'];
                    }else {
                        $data[$column] = null;
                    }
                }else {
                    throw new Exception("No value provided for column $column", 1);
                }
            }
        }

        $this->tables[$table]['data'][] = $data;

        $this->commit($table);
    }

    public function select($table, $args = null) {
        $this->get_table($table);
        if (is_null($args)) {
            return $this->tables[$table]['data'];
        }
        $data = $this->tables[$table]['data'];
        $result = [];

        $validations = $this->column_exists($table, array_keys($args));

        foreach ($validations as $key => $value)
            if (false === $value)
                throw new Exception("Column $key not found", 1);

        foreach ($data as $item) {
            $args_1 = 0;
            foreach ($args as $column => $value) {
                if ($item[$column] === $value)
                    $args_1++;
            }

            if ($args_1 >= count($args))
                $result[] = $item;
        }

        return $result;

    }

    public function update($table, $update, $statement = null) {
        $this->get_table($table);
        
        $data = $this->tables[$table]['data'];

        if (!is_null($statement)) {
            $validations = $this->column_exists($table, array_keys($statement));

            foreach ($validations as $key => $value)
                if (false === $value)
                    throw new Exception("Column $key not found", 1);

        }

        $validations = $this->column_exists($table, array_keys($update));

        foreach ($validations as $key => $value)
            if (false === $value)
                throw new Exception("Column $key not found", 1);
        
        foreach ($data as $key => $item) {
            $args_1 = 0;
            if (!is_null($statement)) {
                foreach ($statement as $column => $value) {
                    if ($item[$column] === $value)
                        $args_1++;
                }
            }else {
                foreach ($update as $column => $value) {
                    if ($item[$column])
                        $args_1++;
                }
            }

            if ($args_1 >= count($update)) {
                $item[$column] = $update[$column];
                $this->tables[$table]['data'][$key] = $item;
            }
        }
        $this->commit($table);
        return $this->tables[$table]['data'];
    }

    public function delete($table, $args = null) {
        $this->get_table($table);
        $data = $this->tables[$table]['data'];

        if (is_null($args)) {
            $this->tables[$table]['data'] = [];
            $this->commit($table);
            return $this->tables[$table]['data'];
        }

        $validations = $this->column_exists($table, array_keys($args));

        foreach ($validations as $key => $value)
            if (false === $value)
                throw new Exception("Column $key not found", 1);
        
        foreach ($data as $key => $item) {
            foreach ($args as $column => $value) {
                if ($item[$column] === $value) {
                    unset($this->tables[$table]['data'][$key]);
                }
            }
        }
        $this->commit($table);
        return $this->tables[$table]['data'];
    }

    private function get_table($table) {
        $table_path = $this->db_file_path . '/' . $table . '.json';
        if (!file_exists($table_path))
            throw new Exception("Table $table not found", 1);
            
        $json = file_get_contents($table_path);
        $this->tables[$table] = json_decode($json, true);
        
        return $this->tables[$table];
    }

    public function column_exists($table, $columns) {
        $this->get_table($table);
        
        $validate = [];
        foreach ($columns as $column) {
            if (array_key_exists($column, $this->tables[$table]['schema'])) {
                $validate[$column] = true;
            }else {
                $validate[$column] = false;
            }
        }
        
        return $validate;
    }

    private function commit($table) {
        $table_path = $this->db_file_path . '/' . $table . '.json';
        $fp = fopen($table_path, 'w');
        fwrite($fp, json_encode($this->tables[$table]));
        fclose($fp);
        $this->get_table($table);
    }
}

