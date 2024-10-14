package repo

import (
	"encoding/json"
	"sync"

	"github.com/jinzhu/gorm"
	"github.com/jinzhu/gorm/dialects/postgres"

	"github.com/moonwalker/bedrock/pkg/rules"
)

const tblName = "rules"

type dbRule struct {
	ID   string
	Rule postgres.Jsonb
}

func (d *dbRule) TableName() string {
	return tblName
}

type postgresRuleRepo struct {
	connectionString string
}

var (
	once sync.Once
	inst *postgresRuleRepo
)

func NewPostgresRuleRepo(connectionString string) (RuleRepo, error) {
	var err error
	// singleton
	once.Do(func() {
		inst = &postgresRuleRepo{connectionString}
		err = inst.init()
	})
	return inst, err
}

func (s *postgresRuleRepo) open() (*gorm.DB, error) {
	db, err := gorm.Open("postgres", s.connectionString)
	if err != nil {
		return nil, err
	}

	return db, nil
}

func (s *postgresRuleRepo) init() error {
	db, err := s.open()
	if err != nil {
		return err
	}
	defer db.Close()

	db.AutoMigrate(&dbRule{})
	return db.Error
}

func (s *postgresRuleRepo) Name() string {
	return "postgres"
}

func (s *postgresRuleRepo) Get(id string) (*rules.Rule, error) {
	db, err := s.open()
	if err != nil {
		return nil, err
	}
	defer db.Close()

	var row dbRule
	db.First(&row, "id = ?", id)
	if db.Error != nil {
		return nil, db.Error
	}

	var rule *rules.Rule
	err = json.Unmarshal(row.Rule.RawMessage, &rule)
	if err != nil {
		return nil, err
	}

	return rule, nil
}

func (s *postgresRuleRepo) Save(rule *rules.Rule) error {
	db, err := s.open()
	if err != nil {
		return err
	}
	defer db.Close()

	buf, err := json.Marshal(rule)
	if err != nil {
		return err
	}

	ruleJSON := json.RawMessage(buf)
	db.Save(&dbRule{
		ID:   rule.ID,
		Rule: postgres.Jsonb{ruleJSON},
	})

	return db.Error
}

func (s *postgresRuleRepo) Expire(id string, ttl int64) error {
	return nil
}

func (s *postgresRuleRepo) Remove(id string) error {
	db, err := s.open()
	if err != nil {
		return err
	}
	defer db.Close()

	db.Delete(&dbRule{ID: id})
	return db.Error
}

func (s *postgresRuleRepo) RemoveAll() error {
	db, err := s.open()
	if err != nil {
		return err
	}
	defer db.Close()

	db.Delete(dbRule{})
	return db.Error
}

func (s *postgresRuleRepo) Each(skip int, limit int, fn func(rule *rules.Rule)) error {
	db, err := s.open()
	if err != nil {
		return err
	}
	defer db.Close()

	query := db.Offset(skip)
	if limit > 0 {
		query.Limit(limit)
	}

	var dbRules []dbRule
	query.Find(&dbRules)
	if db.Error != nil {
		return db.Error
	}

	for _, row := range dbRules {
		var rule *rules.Rule
		err = json.Unmarshal(row.Rule.RawMessage, &rule)
		if err != nil {
			return err
		}
		fn(rule)
	}

	return db.Error
}

func (s *postgresRuleRepo) Count() (count int) {
	db, err := s.open()
	if err != nil {
		return
	}
	defer db.Close()

	db.Table(tblName).Count(&count)
	return
}

func (s *postgresRuleRepo) Active() (active int) {
	db, err := s.open()
	if err != nil {
		return
	}
	defer db.Close()

	db.Model(&dbRule{}).Where("rule->'active' = ?", true).Count(&active)
	return
}

func (s *postgresRuleRepo) Close() {
	// no op
}
