package event

var (
	userRegisteredTopic = "UserRegistered"
)

type UserRegistered struct {
	Email     string
	Password  string
	FirstName string
	LastName  string
}

func (e *UserRegistered) GetTopic() string {
	return userRegisteredTopic
}