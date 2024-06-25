package control

import "github.com/drkisler/dataPedestal/plugin/pullPlugin/manager/module"

func OpenDB() (*module.TStorage, error) {
	db, err := module.GetDbServ()
	if err != nil {
		return nil, err
	}
	if err = db.OpenDB(); err != nil {
		return nil, err
	}
	return db, nil
}
func CloseDB() error {
	db, err := module.GetDbServ()
	if err != nil {
		return err
	}

	return db.CloseDB()
}
